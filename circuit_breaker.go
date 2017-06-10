package rabbus

import (
	"time"

	"github.com/rafaeljesus/retry-go"
	"github.com/rubyist/circuitbreaker"
)

type function func() error

type breaker struct {
	provider *circuit.Breaker
	attempts int
	sleep    time.Duration
}

func newThresholdBreaker(threshold int64, attempts int, sleep time.Duration) *breaker {
	s := time.Second * time.Duration(sleep)
	cb := circuit.NewThresholdBreaker(threshold)
	return &breaker{cb, attempts, s}
}

func (b *breaker) call(fn function, timeout time.Duration) error {
	return retry.Do(func() error {
		return b.provider.Call(func() error {
			return fn()
		}, timeout)
	}, b.attempts, b.sleep)
}
