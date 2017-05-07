package rabbus

import (
	"errors"
)

var (
	// ErrMissingExchangeName is returned when exchange name is not passed as parameter.
	ErrMissingExchangeName = errors.New("Missing field exchange_name")
	// ErrMissingExchangeType is returned when exchange type is not passed as parameter.
	ErrMissingExchangeType = errors.New("Missing field exchange_type")
	// ErrMissingQueueName is returned when queue name is not passed as parameter.
	ErrMissingQueueName = errors.New("Missing field queue_name")
	// ErrMissingHandlerFunc is returned when function handler is not passed as parameter.
	ErrMissingHandlerFunc = errors.New("Missing field handler_func")
)
