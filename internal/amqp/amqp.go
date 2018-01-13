package amqp

import (
	"github.com/streadway/amqp"
)

// Amqp expose a interface for interacting with amqp broker
type Amqp interface {
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

// AmqpInterpreter interpret (implement) Amqp interface definition
type AmqpInterpreter struct {
	conn            *amqp.Connection
	ch              *amqp.Channel
	passiveExchange bool
}

// New returns a new AmqpInterpreter configured, or returning an non-nil err
// if an error occurred while creating connection or channel.
func New(dsn string, pex bool) (*AmqpInterpreter, error) {
	conn, ch, err := createConnAndChan(dsn)
	if err != nil {
		return nil, err
	}

	return &AmqpInterpreter{
		conn:            conn,
		ch:              ch,
		passiveExchange: pex,
	}, nil
}

// Publish wraps amqp.Publish method
func (ai *AmqpInterpreter) Publish(exchange, key string, opts amqp.Publishing) error {
	return ai.ch.Publish(exchange, key, false, false, opts)
}

// CreateConsumer creates a amqp consumer
func (ai *AmqpInterpreter) CreateConsumer(exchange, key, kind, queue string, durable bool) (<-chan amqp.Delivery, error) {
	if err := ai.WithExchange(exchange, kind, durable); err != nil {
		return nil, err
	}

	q, err := ai.ch.QueueDeclare(queue, durable, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	if err := ai.ch.QueueBind(q.Name, key, exchange, false, nil); err != nil {
		return nil, err
	}

	return ai.ch.Consume(q.Name, "", false, false, false, false, nil)
}

// WithExchange creates a amqp exchange
func (ai *AmqpInterpreter) WithExchange(exchange, kind string, durable bool) error {
	if ai.passiveExchange {
		return ai.ch.ExchangeDeclarePassive(exchange, kind, durable, false, false, false, nil)
	}

	return ai.ch.ExchangeDeclare(exchange, kind, durable, false, false, false, nil)
}

// WithQos wrapper over amqp.Qos method
func (ai *AmqpInterpreter) WithQos(count, size int, global bool) error {
	return ai.ch.Qos(count, size, global)
}

// NotifyClose wrapper over notifyClose method
func (ai *AmqpInterpreter) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return ai.conn.NotifyClose(c)
}

// Close closes the running amqp connection and channel
func (ai *AmqpInterpreter) Close() error {
	if err := ai.ch.Close(); err != nil {
		return err
	}

	if ai.conn != nil {
		return ai.conn.Close()
	}

	return nil
}

func createConnAndChan(dsn string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(dsn)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return conn, ch, nil
}
