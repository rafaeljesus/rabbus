package rabbus

import (
	"sync"
	"time"

	amqpwrap "github.com/rafaeljesus/rabbus/internal/amqp"
	"github.com/rafaeljesus/retry-go"
	"github.com/sony/gobreaker"
	"github.com/streadway/amqp"
)

const (
	// Transient means higher throughput but messages will not be restored on broker restart.
	Transient uint8 = 1
	// Persistent messages will be restored to durable queues and lost on non-durable queues during server restart.
	Persistent uint8 = 2
	// ContentTypeJSON define json content type
	ContentTypeJSON = "application/json"
	// ContentTypePlain define plain text content type
	ContentTypePlain = "plain/text"

	contentEncoding = "UTF-8"
)

type (
	// Config carries the variables to tune a newly started rabbus.
	Config struct {
		// Dsn is the amqp url address.
		Dsn string
		// Durable indicates of the queue will survive broker restarts. Default to true.
		Durable bool
		// PassiveExchange forces passive connection with all exchanges using
		// amqp's ExchangeDeclarePassive instead the default ExchangeDeclare
		PassiveExchange bool
		// Qos controls how many messages or how many bytes will be consumed before receiving delivery acks
		Qos
		// Retry settings for the in memory retry mechanism
		Retry
		// Breaker circuit breaker configuration
		Breaker
	}

	// Retry config for the in memory retry mechanism
	Retry struct {
		// Attempts is the max number of retries on broker outages.
		Attempts int
		// Sleep is the sleep time of the retry mechanism.
		Sleep time.Duration

		reconnectSleep time.Duration
	}

	// Breaker carries the configuration for circuit breaker
	Breaker struct {
		// Interval is the cyclic period of the closed state for CircuitBreaker to clear the internal counts,
		// If Interval is 0, CircuitBreaker doesn't clear the internal counts during the closed state.
		Interval time.Duration
		// Timeout is the period of the open state, after which the state of CircuitBreaker becomes half-open.
		// If Timeout is 0, the timeout value of CircuitBreaker is set to 60 seconds.
		Timeout time.Duration
		// Threshold when a threshold of failures has been reached, future calls to the broker will not run.
		// During this state, the circuit breaker will periodically allow the calls to run and, if it is successful,
		// will start running the function again. Default value is 5.
		Threshold uint32
		// OnStateChange is called whenever the state of CircuitBreaker changes.
		OnStateChange func(name, from, to string)
	}

	// Qos controls how many messages or how many bytes the server will try to keep on the network for consumers before receiving delivery acks.
	Qos struct {
		PrefetchCount int
		PrefetchSize  int
		Global        bool
	}

	// Message carries fields for sending messages.
	Message struct {
		// Exchange the exchange name.
		Exchange string
		// Kind the exchange type.
		Kind string
		// Key the routing key name.
		Key string
		// Payload the message payload.
		Payload []byte
		// DeliveryMode indicates if the is Persistent or Transient.
		DeliveryMode uint8
		// ContentType the message content-type.
		ContentType string
		// Headers the message application headers
		Headers map[string]interface{}
	}

	// ListenConfig carries fields for listening messages.
	ListenConfig struct {
		// Exchange the exchange name.
		Exchange string
		// Kind the exchange type.
		Kind string
		// Key the routing key name.
		Key string
		// PassiveExchange determines a passive exchange connection it uses
		// amqp's ExchangeDeclarePassive instead the default ExchangeDeclare
		PassiveExchange bool
		// Queue the queue name
		Queue string
	}

	// Delivery wraps amqp.Delivery struct
	Delivery struct {
		amqp.Delivery
	}

	// Rabbus interpret (implement) Rabbus interface definition
	Rabbus struct {
		mu         sync.RWMutex
		amqp       Amqp
		breaker    *gobreaker.CircuitBreaker
		emit       chan Message
		emitErr    chan error
		emitOk     chan struct{}
		reconn     chan struct{}
		config     Config
		exDeclared map[string]struct{}
	}

	// Amqp expose a interface for interacting with amqp broker
	Amqp interface {
		// Publish wraps amqp.Publish method
		Publish(exchange, key string, opts amqp.Publishing) error
		// CreateConsumer creates a amqp consumer
		CreateConsumer(exchange, key, kind, queue string, durable bool) (<-chan amqp.Delivery, error)
		// WithExchange creates a amqp exchange
		WithExchange(exchange, kind string, durable bool) error
		// WithQos wrapper over amqp.Qos method
		WithQos(count, size int, global bool) error
		// NotifyClose wrapper over notifyClose method
		NotifyClose(c chan *amqp.Error) chan *amqp.Error
		// Close closes the running amqp connection and channel
		Close() error
	}
)

func (lc ListenConfig) validate() error {
	if lc.Exchange == "" {
		return ErrMissingExchange
	}

	if lc.Kind == "" {
		return ErrMissingKind
	}

	if lc.Queue == "" {
		return ErrMissingQueue
	}

	return nil
}

// NewRabbus returns a new Rabbus configured with the
// variables from the config parameter, or returning an non-nil err
// if an error occurred while creating connection and channel.
func NewRabbus(c Config, amqpProvider ...Amqp) (*Rabbus, error) {
	if len(amqpProvider) > 1 {
		return nil, ErrUnsupportedArguments
	}

	var amqpWrapper Amqp
	if len(amqpProvider) == 0 {
		var err error
		amqpWrapper, err = amqpwrap.New(c.Dsn, c.PassiveExchange)
		if err != nil {
			return nil, err
		}
	} else {
		amqpWrapper = amqpProvider[0]
	}

	if err := amqpWrapper.WithQos(c.Qos.PrefetchCount, c.Qos.PrefetchSize, c.Qos.Global); err != nil {
		return nil, err
	}

	if c.Threshold == 0 {
		c.Threshold = 5
	}

	if c.Retry.Sleep == 0 {
		c.Retry.reconnectSleep = time.Second * 10
	}

	ri := newRabbus(amqpWrapper, c)

	go ri.register()
	go ri.listenClose()

	return ri, nil
}

// EmitAsync emits a message to RabbitMQ, but does not wait for the response from broker.
func (ri *Rabbus) EmitAsync() chan<- Message {
	return ri.emit
}

// EmitErr returns an error if encoding payload fails, or if after circuit breaker is open or retries attempts exceed.
func (ri *Rabbus) EmitErr() <-chan error {
	return ri.emitErr
}

// EmitOk returns true when the message was sent.
func (ri *Rabbus) EmitOk() <-chan struct{} {
	return ri.emitOk
}

// Listen to a message from RabbitMQ, returns
// an error if exchange, queue name and function handler not passed or if an error occurred while creating
// amqp consumer.
func (ri *Rabbus) Listen(c ListenConfig) (chan ConsumerMessage, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}

	msgs, err := ri.amqp.CreateConsumer(c.Exchange, c.Key, c.Kind, c.Queue, ri.config.Durable)
	if err != nil {
		return nil, err
	}

	ri.mu.Lock()
	ri.exDeclared[c.Exchange] = struct{}{}
	ri.mu.Unlock()

	messages := make(chan ConsumerMessage, 256)
	go ri.wrapMessage(c, msgs, messages)
	go ri.listenReconn(c, messages)

	return messages, nil
}

// Close attempt to close channel and connection.
func (ri *Rabbus) Close() error {
	close(ri.emit)
	close(ri.emitOk)
	close(ri.emitErr)
	close(ri.reconn)

	return ri.amqp.Close()
}

func (ri *Rabbus) register() {
	for m := range ri.emit {
		ri.produce(m)
	}
}

func (ri *Rabbus) produce(m Message) {
	if _, ok := ri.exDeclared[m.Exchange]; !ok {
		if err := ri.amqp.WithExchange(m.Exchange, m.Kind, ri.config.Durable); err != nil {
			ri.emitErr <- err
			return
		}
		ri.exDeclared[m.Exchange] = struct{}{}
	}

	if m.ContentType == "" {
		m.ContentType = ContentTypeJSON
	}

	if m.DeliveryMode == 0 {
		m.DeliveryMode = Persistent
	}

	opts := amqp.Publishing{
		Headers:         amqp.Table(m.Headers),
		ContentType:     m.ContentType,
		ContentEncoding: contentEncoding,
		DeliveryMode:    m.DeliveryMode,
		Timestamp:       time.Now(),
		Body:            m.Payload,
	}

	if _, err := ri.breaker.Execute(func() (interface{}, error) {
		return nil, retry.Do(func() error {
			return ri.amqp.Publish(m.Exchange, m.Key, opts)
		}, ri.config.Retry.Attempts, ri.config.Retry.Sleep)
	}); err != nil {
		ri.emitErr <- err
		return
	}

	ri.emitOk <- struct{}{}
}

func (ri *Rabbus) wrapMessage(c ListenConfig, sourceChan <-chan amqp.Delivery, targetChan chan ConsumerMessage) {
	for m := range sourceChan {
		targetChan <- newConsumerMessage(m)
	}
}

func (ri *Rabbus) listenClose() {
	if err := <-ri.amqp.NotifyClose(make(chan *amqp.Error)); err != nil {
		for {
			time.Sleep(ri.config.Retry.reconnectSleep)

			aw, err := amqpwrap.New(ri.config.Dsn, ri.config.PassiveExchange)
			if err != nil {
				continue
			}

			ri.mu.Lock()
			ri.amqp = aw
			ri.mu.Unlock()

			go ri.listenClose()

			ri.reconn <- struct{}{}

			break
		}
	}
}

func (ri *Rabbus) listenReconn(c ListenConfig, messages chan ConsumerMessage) {
	for range ri.reconn {
		msgs, err := ri.amqp.CreateConsumer(c.Exchange, c.Key, c.Kind, c.Queue, ri.config.Durable)
		if err != nil {
			ri.amqp.Close()
			continue
		}

		go ri.wrapMessage(c, msgs, messages)
		go ri.listenReconn(c, messages)
		break
	}
}

func newRabbus(am Amqp, c Config) *Rabbus {
	return &Rabbus{
		amqp:       am,
		config:     c,
		breaker:    gobreaker.NewCircuitBreaker(newBreakerSettings(c.Breaker)),
		emit:       make(chan Message),
		emitErr:    make(chan error),
		emitOk:     make(chan struct{}),
		reconn:     make(chan struct{}),
		exDeclared: make(map[string]struct{}),
	}
}

func newBreakerSettings(c Breaker) gobreaker.Settings {
	return gobreaker.Settings{
		Name:     "rabbus-circuit-breaker",
		Interval: c.Interval,
		Timeout:  c.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures > c.Threshold
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			c.OnStateChange(name, from.String(), to.String())
		},
	}
}
