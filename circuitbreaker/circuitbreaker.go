package circuit

import (
	"time"

	"github.com/rafaeljesus/retry-go"
	"github.com/rubyist/circuitbreaker"
)

type function func() error

type Breaker interface {
	Call(fn function, timeout time.Duration) error
}

type breaker struct {
	provider *circuit.Breaker
	attempts int
	sleep    time.Duration
}

func NewThresholdBreaker(threshold int64, attempts int, sleep time.Duration) Breaker {
	s := time.Second * time.Duration(sleep)
	cb := circuit.NewThresholdBreaker(threshold)
	return &breaker{cb, attempts, s}
}

func (b *breaker) Call(fn function, timeout time.Duration) error {
	return retry.Do(func() error {
		return b.provider.Call(func() error {
			return fn()
		}, timeout)
	}, b.attempts, b.sleep)
}
