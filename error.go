package rabbus

import (
	"errors"
)

var (
	// ErrMissingExchange is returned when exchange name is not passed as parameter.
	ErrMissingExchange = errors.New("Missing field exchange")
	// ErrMissingKind is returned when exchange type is not passed as parameter.
	ErrMissingKind = errors.New("Missing field kind")
	// ErrMissingQueue is returned when queue name is not passed as parameter.
	ErrMissingQueue = errors.New("Missing field queue")
	// ErrMissingHandler is returned when function handler is not passed as parameter.
	ErrMissingHandler = errors.New("Missing field handler")
	// ErrUnsupportedArguments is returned when more than the permitted arguments is passed to a function.
	ErrUnsupportedArguments = errors.New("Unsupported arguments size")
)
