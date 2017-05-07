package rabbus

import (
	"encoding/json"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
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
}

// Message carries fields for sending messages.
type Message struct {
	ExchangeName string
	ExchangeType string
	RoutingKey   string
	Payload      interface{}
	DeliveryMode uint8
}

type handlerFunc func(d *Delivery)

// ListenConfig carries fields for listening messages.
type ListenConfig struct {
	ExchangeName string
	ExchangeType string
	RoutingKey   string
	QueueName    string
	Durable      bool
	HandlerFunc  handlerFunc
}

// Delivery wraps amqp.Delivery struct
type Delivery struct {
	amqp.Delivery
}

type rabbus struct {
	sync.RWMutex
	conn       *amqp.Connection
	ch         *amqp.Channel
	emitter    chan *Message
	emitterErr chan error
	emitterOk  chan bool
	config     Config
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

	ra := &rabbus{
		conn:       conn,
		ch:         ch,
		emitter:    make(chan *Message),
		emitterErr: make(chan error),
		emitterOk:  make(chan bool),
		config:     c,
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
	if c.ExchangeName == "" {
		err = ErrMissingExchangeName
		return
	}

	if c.ExchangeType == "" {
		err = ErrMissingExchangeType
		return
	}

	if c.QueueName == "" {
		err = ErrMissingQueueName
		return
	}

	if c.HandlerFunc == nil {
		err = ErrMissingHandlerFunc
		return
	}

	l := log.WithFields(log.Fields{
		"exchange_name": c.ExchangeName,
		"routing_key":   c.RoutingKey,
		"queue":         c.QueueName,
	})

	if err = r.ch.ExchangeDeclare(c.ExchangeName, c.ExchangeType, false, false, false, false, nil); err != nil {
		return
	}

	l.Debug("Declaring queue")
	q, err := r.ch.QueueDeclare(c.QueueName, c.Durable, false, false, false, nil)
	if err != nil {
		return
	}

	l.Debug("Binding queue")
	if err = r.ch.QueueBind(q.Name, c.RoutingKey, c.ExchangeName, false, nil); err != nil {
		return
	}

	l.Debug("Adding consumer")
	messages, err := r.ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return
	}

	go func(l *log.Entry) {
		for {
			select {
			case m := <-messages:
				l.Debug("Receiving message")

				d := &Delivery{Delivery: m}
				c.HandlerFunc(d)
			}
		}
	}(l)

	return
}

// Close attempt to close channel and connection.
func (r *rabbus) Close() {
	r.ch.Close()
	r.conn.Close()
}

func (r *rabbus) register() {
	go func() {
		for {
			select {
			case m := <-r.emitter:
				r.emit(m)
			}
		}
	}()
}

func (r *rabbus) emit(m *Message) {
	cb := circuit.NewThresholdBreaker(r.config.Attempts)
	l := log.WithField("routing_key", m.RoutingKey)

	l.Debug("Sending message")

	err := cb.Call(func() (err error) {
		if err = r.ch.ExchangeDeclare(m.ExchangeName, m.ExchangeType, false, false, false, false, nil); err != nil {
			return
		}

		body, err := json.Marshal(m.Payload)
		if err != nil {
			return
		}

		message := amqp.Publishing{
			DeliveryMode:    m.DeliveryMode,
			Timestamp:       time.Now(),
			ContentEncoding: "UTF-8",
			ContentType:     "application/json",
			Body:            body,
		}

		err = r.ch.Publish(m.ExchangeName, m.RoutingKey, false, false, message)

		return

	}, r.config.Timeout)

	if err != nil {
		l.WithError(err).Error("Failed to send message")
		r.emitterErr <- err
		return
	}

	r.emitterOk <- true

	l.Debug("Message successfully sent")
}

func (r *rabbus) notifyClose() {
	shutdownError := <-r.conn.NotifyClose(make(chan *amqp.Error))

	if shutdownError != nil {
		log.WithField("timeout", r.config.Timeout).Info("Caught RabbitMQ close notification with error, trying to reconnect")
		r.reconnect()
	}
}

func (r *rabbus) reconnect() {
	for {
		time.Sleep(r.config.Timeout)
		l := log.WithField("timeout", r.config.Timeout)

		conn, err := amqp.Dial(r.config.Dsn)
		if err != nil {
			l.WithError(err).Error("Failed to open new connection, will try later")
			return
		}

		ch, err := conn.Channel()
		if err != nil {
			l.WithError(err).Error("Failed to open a new channel, will try later")
			return
		}

		r.Lock()
		defer r.Unlock()

		r.conn = conn
		r.ch = ch

		go r.notifyClose()

		break
	}
}
