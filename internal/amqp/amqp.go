package amqp

import "github.com/streadway/amqp"

type (
	// Amqp interpret (implement) Amqp interface definition
	Amqp struct {
		conn            *amqp.Connection
		ch              *amqp.Channel
		passiveExchange bool
	}
)

// New returns a new Amqp configured, or returning an non-nil err
// if an error occurred while creating connection or channel.
func New(dsn string, pex bool) (*Amqp, error) {
	conn, ch, err := createConnAndChan(dsn)
	if err != nil {
		return nil, err
	}

	return &Amqp{
		conn:            conn,
		ch:              ch,
		passiveExchange: pex,
	}, nil
}

// Publish wraps amqp.Publish method
func (ai *Amqp) Publish(exchange, key string, opts amqp.Publishing) error {
	return ai.ch.Publish(exchange, key, false, false, opts)
}

// CreateConsumer creates a amqp consumer
func (ai *Amqp) CreateConsumer(exchange, key, kind, queue string, durable bool) (<-chan amqp.Delivery, error) {
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
func (ai *Amqp) WithExchange(exchange, kind string, durable bool) error {
	if ai.passiveExchange {
		return ai.ch.ExchangeDeclarePassive(exchange, kind, durable, false, false, false, nil)
	}

	return ai.ch.ExchangeDeclare(exchange, kind, durable, false, false, false, nil)
}

// WithQos wrapper over amqp.Qos method
func (ai *Amqp) WithQos(count, size int, global bool) error {
	return ai.ch.Qos(count, size, global)
}

// NotifyClose wrapper over notifyClose method
func (ai *Amqp) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return ai.conn.NotifyClose(c)
}

// Close closes the running amqp connection and channel
func (ai *Amqp) Close() error {
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
