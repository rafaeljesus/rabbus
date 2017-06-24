package rabbus

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/rafaeljesus/rabbus/circuitbreaker"
	"github.com/streadway/amqp"
)

const (
	// Transient means higher throughput but messages will not be restored on broker restart.
	Transient uint8 = 1
	// Persistent messages will be restored to durable queues and lost on non-durable queues during server restart.
	Persistent uint8 = 2
)

// Rabbus exposes a interface for emitting and listening for messages.
type Rabbus interface {
	// EmitAsync emits a message to RabbitMQ, but does not wait for the response from broker.
	EmitAsync() chan<- Message
	// EmitErr returns an error if encoding payload fails, or if after circuit breaker is open or retries attempts exceed.
	EmitErr() <-chan error
	// EmitOk returns true when the message was sent.
	EmitOk() <-chan struct{}
	// Listen to a message from RabbitMQ, returns
	// an error if exchange, queue name and function handler not passed or if an error occurred while creating
	// amqp consumer.
	Listen(ListenConfig) (chan ConsumerMessage, error)
	// Close attempt to close channel and connection.
	Close()
}

// Config carries the variables to tune a newly started rabbus.
type Config struct {
	// Dsn is the amqp url address.
	Dsn string
	// Attempts is the max number of retries on broker outages.
	Attempts int
	// Threshold when a threshold of failures has been reached, future calls to the broker will not run.
	// During this state, the circuit breaker will periodically allow the calls to run and, if it is successful,
	// will start running the function again
	Threshold int64
	// Timeout when a timeout has been reached, future calls to the broker will not run.
	// During this state, the circuit breaker will periodically allow the calls to run and, if it is successful,
	// will start running the function again
	Timeout time.Duration
	// Sleep is the sleep time of the retry mechanism.
	Sleep time.Duration
	// Durable indicates of the queue will survive broker restarts. Default to true.
	Durable bool
}

// Message carries fields for sending messages.
type Message struct {
	// Exchange the exchange name.
	Exchange string
	// Kind the exchange type.
	Kind string
	// Key the routing key name.
	Key string
	// Payload the message payload.
	Payload interface{}
	// DeliveryMode indicates if the is Persistent or Transient.
	DeliveryMode uint8
}

// ListenConfig carries fields for listening messages.
type ListenConfig struct {
	// Exchange the exchange name.
	Exchange string
	// Kind the exchange type.
	Kind string
	// Key the routing key name.
	Key string
	// Queue the queue name
	Queue string
}

// Delivery wraps amqp.Delivery struct
type Delivery struct {
	amqp.Delivery
}

type rabbus struct {
	sync.RWMutex
	conn           *amqp.Connection
	ch             *amqp.Channel
	circuitbreaker circuit.Breaker
	emit           chan Message
	emitErr        chan error
	emitOk         chan struct{}
	config         Config
}

// NewRabbus returns a new Rabbus configured with the
// variables from the config parameter, or returning an non-nil err
// if an error occurred while creating connection and channel.
func NewRabbus(c Config) (Rabbus, error) {
	conn, err := amqp.Dial(c.Dsn)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	r := &rabbus{
		conn:           conn,
		ch:             ch,
		circuitbreaker: circuit.NewThresholdBreaker(c.Threshold, c.Attempts, c.Sleep),
		emit:           make(chan Message),
		emitErr:        make(chan error),
		emitOk:         make(chan struct{}),
		config:         c,
	}

	go r.register()
	go notifyClose(c.Dsn, r)

	rab := r

	return rab, nil
}

// EmitAsync emits a message to RabbitMQ, but does not wait for the response from broker.
func (r *rabbus) EmitAsync() chan<- Message {
	return r.emit
}

// EmitErr returns an error if encoding payload fails, or if after circuit breaker is open or retries attempts exceed.
func (r *rabbus) EmitErr() <-chan error {
	return r.emitErr
}

// EmitOk returns true when the message was sent.
func (r *rabbus) EmitOk() <-chan struct{} {
	return r.emitOk
}

// Listen to a message from RabbitMQ, returns
// an error if exchange, queue name and function handler not passed or if an error occurred while creating
// amqp consumer.
func (r *rabbus) Listen(c ListenConfig) (chan ConsumerMessage, error) {
	if c.Exchange == "" {
		return nil, ErrMissingExchange
	}

	if c.Kind == "" {
		return nil, ErrMissingKind
	}

	if c.Queue == "" {
		return nil, ErrMissingQueue
	}

	if err := r.ch.ExchangeDeclare(c.Exchange, c.Kind, r.config.Durable, false, false, false, nil); err != nil {
		return nil, err
	}

	q, err := r.ch.QueueDeclare(c.Queue, r.config.Durable, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	if err := r.ch.QueueBind(q.Name, c.Key, c.Exchange, false, nil); err != nil {
		return nil, err
	}

	msgs, err := r.ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	messages := make(chan ConsumerMessage, 256)
	go func(msgs <-chan amqp.Delivery, messages chan ConsumerMessage) {
		for m := range msgs {
			messages <- newConsumerMessage(m)
		}
	}(msgs, messages)

	return messages, nil
}

// Close attempt to close channel and connection.
func (r *rabbus) Close() {
	r.ch.Close()
	r.conn.Close()
}

func (r *rabbus) register() {
	for m := range r.emit {
		r.produce(m)
	}
}

func (r *rabbus) produce(m Message) {
	err := r.circuitbreaker.Call(func() error {
		body, err := json.Marshal(m.Payload)
		if err != nil {
			return err
		}

		if m.DeliveryMode == 0 {
			m.DeliveryMode = Persistent
		}

		return r.ch.Publish(m.Exchange, m.Key, false, false, amqp.Publishing{
			ContentType:     "application/json",
			ContentEncoding: "UTF-8",
			DeliveryMode:    m.DeliveryMode,
			Timestamp:       time.Now(),
			Body:            body,
		})
	}, r.config.Timeout)

	if err != nil {
		r.emitErr <- err
		return
	}

	r.emitOk <- struct{}{}
}

func notifyClose(dsn string, r *rabbus) {
	err := <-r.conn.NotifyClose(make(chan *amqp.Error))
	if err != nil {
		for {
			time.Sleep(time.Second * 2)
			conn, err := amqp.Dial(dsn)
			if err != nil {
				continue
			}

			ch, err := conn.Channel()
			if err != nil {
				continue
			}

			r.Lock()
			defer r.Unlock()
			r.conn = conn
			r.ch = ch

			go notifyClose(dsn, r)

			break
		}
	}
}
