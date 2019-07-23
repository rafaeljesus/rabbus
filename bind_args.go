package rabbus

import (
	"github.com/streadway/amqp"
)

// BindArgs is the wrapper for AMQP Table class to set common queue bind values
type BindArgs struct {
	args amqp.Table
}

// NewBindArgs creates new queue bind values builder
func NewBindArgs() *BindArgs {
	return &BindArgs{args: make(amqp.Table)}
}

// With sets the value by name
func (a *BindArgs) With(name string, value interface{}) *BindArgs {
	a.args[name] = value
	return a
}
