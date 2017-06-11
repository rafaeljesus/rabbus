package circuit

import (
	"time"

	"github.com/rafaeljesus/retry-go"
	"github.com/rubyist/circuitbreaker"
)

type function func() error

// Breaker exposes a interface with the circuit breaker public methods.
type Breaker interface {
	Call(fn function, timeout time.Duration) error
}

type breaker struct {
	provider *circuit.Breaker
	attempts int
	sleep    time.Duration
}

// NewThresholdBreaker creates a circuit breaker.
func NewThresholdBreaker(threshold int64, attempts int, sleep time.Duration) Breaker {
	s := time.Second * time.Duration(sleep)
	cb := circuit.NewThresholdBreaker(threshold)
	return &breaker{cb, attempts, s}
}

// Call wraps the function protected by the circuit breaker with a retry mechanism.
// A failure is recorded whenever the function returns an error. If the called function takes longer
// than timeout to run, a failure will be recorded.
func (b *breaker) Call(fn function, timeout time.Duration) error {
	return retry.Do(func() error {
		return b.provider.Call(func() error {
			return fn()
		}, timeout)
	}, b.attempts, b.sleep)
}
