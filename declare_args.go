package rabbus

import (
	"time"

	"github.com/streadway/amqp"
)

const (
	messageTTL = "x-message-ttl"
)

// DeclareArgs is the queue declaration values builder
type DeclareArgs struct {
	args amqp.Table
}

// NewDeclareArgs creates new queue declaration values builder
func NewDeclareArgs() *DeclareArgs {
	return &DeclareArgs{args: make(amqp.Table)}
}

// WithMessageTTL sets Queue message TTL. See details at https://www.rabbitmq.com/ttl.html#message-ttl-using-x-args
func (a *DeclareArgs) WithMessageTTL(d time.Duration) *DeclareArgs {
	// RabbitMQ requires time in milliseconds and duration is in Nanosecond
	return a.With(messageTTL, int64(d/time.Millisecond))
}

// With sets the value by name
func (a *DeclareArgs) With(name string, value interface{}) *DeclareArgs {
	a.args[name] = value
	return a
}
