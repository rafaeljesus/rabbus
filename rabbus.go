package rabbus

import (
	"context"
	"errors"
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
	// ContentTypeJSON define json content type.
	ContentTypeJSON = "application/json"
	// ContentTypePlain define plain text content type.
	ContentTypePlain = "plain/text"
	// ExchangeDirect indicates the exchange is of direct type.
	ExchangeDirect = "direct"
	// ExchangeFanout indicates the exchange is of fanout type.
	ExchangeFanout = "fanout"
	// ExchangeTopic indicates the exchange is of topic type.
	ExchangeTopic = "topic"

	contentEncoding = "UTF-8"
)

type (
	// Option represents an option you can pass to New.
	// See the documentation for the individual options.
	Option func(*Rabbus) error
	// OnStateChangeFunc is the callback function when circuit breaker state changes.
	OnStateChangeFunc func(name, from, to string)

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
		// ContentEncoding the message encoding.
		ContentEncoding string
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
		Amqp
		mu         sync.RWMutex
		breaker    *gobreaker.CircuitBreaker
		emit       chan Message
		emitErr    chan error
		emitOk     chan struct{}
		reconn     chan struct{}
		exDeclared map[string]struct{}
		config
		conDeclared int // conDeclared is a counter for the declared consumers
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

	config struct {
		dsn                string
		durable, passiveex bool
		retrycfg
		breaker
		qos
	}

	retrycfg struct {
		attempts              int
		sleep, reconnectSleep time.Duration
	}

	breaker struct {
		interval, timeout time.Duration
		threshold         uint32
		onStateChange     OnStateChangeFunc
	}

	qos struct {
		prefetchCount, prefetchSize int
		global                      bool
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

// New returns a new Rabbus configured with the
// variables from the config parameter, or returning an non-nil err
// if an error occurred while creating connection and channel.
func New(dsn string, options ...Option) (*Rabbus, error) {
	r := &Rabbus{
		emit:       make(chan Message),
		emitErr:    make(chan error),
		emitOk:     make(chan struct{}),
		reconn:     make(chan struct{}),
		exDeclared: make(map[string]struct{}),
	}

	for _, o := range options {
		if err := o(r); err != nil {
			return nil, err
		}
	}

	if r.Amqp == nil {
		amqpWrapper, err := amqpwrap.New(dsn, r.config.passiveex)
		if err != nil {
			return nil, err
		}
		r.Amqp = amqpWrapper
	}

	if err := r.WithQos(
		r.config.qos.prefetchCount,
		r.config.qos.prefetchSize,
		r.config.qos.global,
	); err != nil {
		return nil, err
	}

	r.config.dsn = dsn
	r.breaker = gobreaker.NewCircuitBreaker(newBreakerSettings(r.config))

	return r, nil
}

// Run starts rabbus channels for emitting and listening for amqp connection close
// returns ctx error in case of any.
func (r *Rabbus) Run(ctx context.Context) error {
	notifyClose := r.NotifyClose(make(chan *amqp.Error))

	for {
		select {
		case m, ok := <-r.emit:
			if !ok {
				return errors.New("unexpected close of emit channel")
			}

			r.produce(m)

		case err := <-notifyClose:
			if err == nil {
				// "… on a graceful close, no error will be sent."
				return nil
			}

			r.handleAmqpClose(err)

			// We have reconnected, so we need a new NotifyClose again.
			notifyClose = r.NotifyClose(make(chan *amqp.Error))

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// EmitAsync emits a message to RabbitMQ, but does not wait for the response from broker.
func (r *Rabbus) EmitAsync() chan<- Message { return r.emit }

// EmitErr returns an error if encoding payload fails, or if after circuit breaker is open or retries attempts exceed.
func (r *Rabbus) EmitErr() <-chan error { return r.emitErr }

// EmitOk returns true when the message was sent.
func (r *Rabbus) EmitOk() <-chan struct{} { return r.emitOk }

// Listen to a message from RabbitMQ, returns
// an error if exchange, queue name and function handler not passed or if an error occurred while creating
// amqp consumer.
func (r *Rabbus) Listen(c ListenConfig) (chan ConsumerMessage, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}

	msgs, err := r.CreateConsumer(c.Exchange, c.Key, c.Kind, c.Queue, r.config.durable)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.conDeclared++ // increase the declared consumers counter
	r.exDeclared[c.Exchange] = struct{}{}
	r.mu.Unlock()

	messages := make(chan ConsumerMessage, 256)
	go r.wrapMessage(c, msgs, messages)
	go r.listenReconn(c, messages)

	return messages, nil
}

// Close channels and attempt to close channel and connection.
func (r *Rabbus) Close() error {
	close(r.emit)
	close(r.emitOk)
	close(r.emitErr)
	close(r.reconn)

	return r.Amqp.Close()
}

func (r *Rabbus) produce(m Message) {
	if _, ok := r.exDeclared[m.Exchange]; !ok {
		if err := r.WithExchange(m.Exchange, m.Kind, r.config.durable); err != nil {
			r.emitErr <- err
			return
		}
		r.exDeclared[m.Exchange] = struct{}{}
	}

	if m.ContentType == "" {
		m.ContentType = ContentTypeJSON
	}

	if m.DeliveryMode == 0 {
		m.DeliveryMode = Persistent
	}

	if m.ContentEncoding == "" {
		m.ContentEncoding = contentEncoding
	}

	opts := amqp.Publishing{
		Headers:         amqp.Table(m.Headers),
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		DeliveryMode:    m.DeliveryMode,
		Timestamp:       time.Now(),
		Body:            m.Payload,
	}

	if _, err := r.breaker.Execute(func() (interface{}, error) {
		return nil, retry.Do(func() error {
			return r.Publish(m.Exchange, m.Key, opts)
		}, r.config.retrycfg.attempts, r.config.retrycfg.sleep)
	}); err != nil {
		r.emitErr <- err
		return
	}

	r.emitOk <- struct{}{}
}

// Durable indicates of the queue will survive broker restarts. Default to true.
func Durable(durable bool) Option {
	return func(r *Rabbus) error {
		r.config.durable = durable
		return nil
	}
}

// PassiveExchange forces passive connection with all exchanges using
// amqp's ExchangeDeclarePassive instead the default ExchangeDeclare
func PassiveExchange(passiveex bool) Option {
	return func(r *Rabbus) error {
		r.config.passiveex = passiveex
		return nil
	}
}

// PrefetchCount limit the number of unacknowledged messages.
func PrefetchCount(count int) Option {
	return func(r *Rabbus) error {
		r.config.qos.prefetchCount = count
		return nil
	}
}

// PrefetchSize when greater than zero, the server will try to keep at least
// that many bytes of deliveries flushed to the network before receiving
// acknowledgments from the consumers.
func PrefetchSize(size int) Option {
	return func(r *Rabbus) error {
		r.config.qos.prefetchSize = size
		return nil
	}
}

// QosGlobal when global is true, these Qos settings apply to all existing and future
// consumers on all channels on the same connection. When false, the Channel.Qos
// settings will apply to all existing and future consumers on this channel.
// RabbitMQ does not implement the global flag.
func QosGlobal(global bool) Option {
	return func(r *Rabbus) error {
		r.config.qos.global = global
		return nil
	}
}

// Attempts is the max number of retries on broker outages.
func Attempts(attempts int) Option {
	return func(r *Rabbus) error {
		r.config.retrycfg.attempts = attempts
		return nil
	}
}

// Sleep is the sleep time of the retry mechanism.
func Sleep(sleep time.Duration) Option {
	return func(r *Rabbus) error {
		if sleep == 0 {
			r.config.retrycfg.reconnectSleep = time.Second * 10
		}
		r.config.retrycfg.sleep = sleep
		return nil
	}
}

// BreakerInterval is the cyclic period of the closed state for CircuitBreaker to clear the internal counts,
// If Interval is 0, CircuitBreaker doesn't clear the internal counts during the closed state.
func BreakerInterval(interval time.Duration) Option {
	return func(r *Rabbus) error {
		r.config.breaker.interval = interval
		return nil
	}
}

// BreakerTimeout is the period of the open state, after which the state of CircuitBreaker becomes half-open.
// If Timeout is 0, the timeout value of CircuitBreaker is set to 60 seconds.
func BreakerTimeout(timeout time.Duration) Option {
	return func(r *Rabbus) error {
		r.config.breaker.timeout = timeout
		return nil
	}
}

// Threshold when a threshold of failures has been reached, future calls to the broker will not run.
// During this state, the circuit breaker will periodically allow the calls to run and, if it is successful,
// will start running the function again. Default value is 5.
func Threshold(threshold uint32) Option {
	return func(r *Rabbus) error {
		if threshold == 0 {
			threshold = 5
		}
		r.config.breaker.threshold = threshold
		return nil
	}
}

// OnStateChange is called whenever the state of CircuitBreaker changes.
func OnStateChange(fn OnStateChangeFunc) Option {
	return func(r *Rabbus) error {
		r.config.breaker.onStateChange = fn
		return nil
	}
}

// AmqpProvider expose a interface for interacting with amqp broker
func AmqpProvider(provider Amqp) Option {
	return func(r *Rabbus) error {
		if provider != nil {
			r.Amqp = provider
			return nil
		}
		return errors.New("unexpected amqp provider")
	}
}

func (r *Rabbus) wrapMessage(c ListenConfig, sourceChan <-chan amqp.Delivery, targetChan chan ConsumerMessage) {
	for m := range sourceChan {
		targetChan <- newConsumerMessage(m)
	}
}

func (r *Rabbus) handleAmqpClose(err error) {
	for {
		time.Sleep(time.Second)
		aw, err := amqpwrap.New(r.config.dsn, r.config.passiveex)
		if err != nil {
			continue
		}

		r.mu.Lock()
		r.Amqp = aw
		r.mu.Unlock()

		if err := r.WithQos(
			r.config.qos.prefetchCount,
			r.config.qos.prefetchSize,
			r.config.qos.global,
		); err != nil {
			r.Amqp.Close()
			continue
		}

		for i := 1; i <= r.conDeclared; i++ {
			r.reconn <- struct{}{}
		}
		break
	}
}

func (r *Rabbus) listenReconn(c ListenConfig, messages chan ConsumerMessage) {
	for range r.reconn {
		msgs, err := r.CreateConsumer(c.Exchange, c.Key, c.Kind, c.Queue, r.config.durable)
		if err != nil {
			continue
		}

		go r.wrapMessage(c, msgs, messages)
		go r.listenReconn(c, messages)
		break
	}
}

func newBreakerSettings(c config) gobreaker.Settings {
	s := gobreaker.Settings{}
	s.Name = "rabbus-circuit-breaker"
	s.Interval = c.breaker.interval
	s.Timeout = c.breaker.timeout
	s.ReadyToTrip = func(counts gobreaker.Counts) bool {
		return counts.ConsecutiveFailures > c.breaker.threshold
	}
	if c.breaker.onStateChange != nil {
		s.OnStateChange = func(name string, from gobreaker.State, to gobreaker.State) {
			c.breaker.onStateChange(name, from.String(), to.String())
		}
	}
	return s
}
