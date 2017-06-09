package rabbus

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/rubyist/circuitbreaker"
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
	Emit() chan<- *Message
	EmitErr() <-chan error
	EmitOk() <-chan bool
	Listen(ListenConfig) error
	Close()

	notifyClose()
	reconnect()
}

// Config carries the variables to tune a newly started rabbus.
type Config struct {
	Dsn      string
	Attempts int64
	Timeout  time.Duration
	Durable  bool
}

// Message carries fields for sending messages.
type Message struct {
	Exchange     string
	Kind         string
	Key          string
	Payload      interface{}
	DeliveryMode uint8
}

type handlerFunc func(d *Delivery)

// ListenConfig carries fields for listening messages.
type ListenConfig struct {
	Exchange string
	Kind     string
	Key      string
	Queue    string
	Handler  handlerFunc
}

// Delivery wraps amqp.Delivery struct
type Delivery struct {
	amqp.Delivery
}

type rabbus struct {
	sync.RWMutex
	conn           *amqp.Connection
	ch             *amqp.Channel
	circuitbreaker *circuit.Breaker
	emitter        chan *Message
	emitterErr     chan error
	emitterOk      chan bool
	config         Config
}

// NewRabbus returns a new Rabbus configured with the
// variables from the config parameter, or returning an non-nil err
// if an error occurred while creating connection and channel.
func NewRabbus(c Config) (r Rabbus, err error) {
	conn, err := amqp.Dial(c.Dsn)
	if err != nil {
		return
	}

	ch, err := conn.Channel()
	if err != nil {
		return
	}

	cb := circuit.NewThresholdBreaker(c.Attempts)
	ra := &rabbus{
		conn:           conn,
		ch:             ch,
		circuitbreaker: cb,
		emitter:        make(chan *Message),
		emitterErr:     make(chan error),
		emitterOk:      make(chan bool),
		config:         c,
	}

	go ra.register()
	go ra.notifyClose()

	r = ra

	return
}

// Emit emits a message to RabbitMQ, but does not wait for the response from broker.
func (r *rabbus) Emit() chan<- *Message {
	return r.emitter
}

// EmitErr returns an error if encoding payload fails, or if after circuit breaker retries attempts exceed
func (r *rabbus) EmitErr() <-chan error {
	return r.emitterErr
}

// EmitOk returns true when the message was sent.
func (r *rabbus) EmitOk() <-chan bool {
	return r.emitterOk
}

// Listen to a message from RabbitMQ, returns
// an error if exchange, queue name and function handler not passed or if an error occurred while creating
// amqp consumer.
func (r *rabbus) Listen(c ListenConfig) (err error) {
	if c.Exchange == "" {
		err = ErrMissingExchange
		return
	}

	if c.Kind == "" {
		err = ErrMissingKind
		return
	}

	if c.Queue == "" {
		err = ErrMissingQueue
		return
	}

	if c.Handler == nil {
		err = ErrMissingHandler
		return
	}

	if err = r.ch.ExchangeDeclare(c.Exchange, c.Kind, r.config.Durable, false, false, false, nil); err != nil {
		return
	}

	q, err := r.ch.QueueDeclare(c.Queue, r.config.Durable, false, false, false, nil)
	if err != nil {
		return
	}

	if err = r.ch.QueueBind(q.Name, c.Key, c.Exchange, false, nil); err != nil {
		return
	}

	messages, err := r.ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return
	}

	go func() {
		for {
			select {
			case m := <-messages:
				d := &Delivery{Delivery: m}
				c.Handler(d)
			}
		}
	}()

	return
}

// Close attempt to close channel and connection.
func (r *rabbus) Close() {
	r.ch.Close()
	r.conn.Close()
}

func (r *rabbus) register() {
	for m := range r.emitter {
		r.emit(m)
	}
}

func (r *rabbus) emit(m *Message) {
	err := r.circuitbreaker.Call(func() error {
		if err := r.ch.ExchangeDeclare(m.Exchange, m.Kind, r.config.Durable, false, false, false, nil); err != nil {
			return err
		}

		body, err := json.Marshal(m.Payload)
		if err != nil {
			return err
		}

		message := amqp.Publishing{
			DeliveryMode:    m.DeliveryMode,
			Timestamp:       time.Now(),
			ContentEncoding: "UTF-8",
			ContentType:     "application/json",
			Body:            body,
		}

		return r.ch.Publish(m.Exchange, m.Key, false, false, message)
	}, r.config.Timeout)

	if err != nil {
		log.Print(err)
		r.emitterErr <- err
		return
	}

	r.emitterOk <- true
}

func (r *rabbus) notifyClose() {
	shutdownError := <-r.conn.NotifyClose(make(chan *amqp.Error))
	if shutdownError != nil {
		r.reconnect()
	}
}

func (r *rabbus) reconnect() {
	for {
		time.Sleep(r.config.Timeout)
		conn, err := amqp.Dial(r.config.Dsn)
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

		go r.notifyClose()

		break
	}
}
