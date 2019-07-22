package rabbus

import (
	"time"

	"github.com/streadway/amqp"
)

// DeclareArgs is the wrapper for AMQP Table class to set common queue declaration values
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
	a.args["x-message-ttl"] = int64(d / time.Millisecond)

	return a
}

// Table returns built args as AMQP Table
func (a *DeclareArgs) Table() amqp.Table {
	return a.args
}
