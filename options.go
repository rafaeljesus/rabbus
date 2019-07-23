package rabbus

import (
	"errors"
	"time"
)

// Option represents an option you can pass to New.
// See the documentation for the individual options.
type Option func(*Rabbus) error

// Durable indicates of the queue will survive broker restarts. Default to true.
func Durable(durable bool) Option {
	return func(r *Rabbus) error {
		r.config.durable = durable
		return nil
	}
}

// PassiveExchange forces passive connection with all exchanges using
// amqp's ExchangeDeclarePassive instead the default ExchangeDeclare
func PassiveExchange(isExchangePassive bool) Option {
	return func(r *Rabbus) error {
		r.config.isExchangePassive = isExchangePassive
		return nil
	}
}

// PrefetchCount limit the number of unacknowledged messages.
func PrefetchCount(count int) Option {
	return func(r *Rabbus) error {
		r.config.qos.prefetchCount = count
		return nil
	}
}

// PrefetchSize when greater than zero, the server will try to keep at least
// that many bytes of deliveries flushed to the network before receiving
// acknowledgments from the consumers.
func PrefetchSize(size int) Option {
	return func(r *Rabbus) error {
		r.config.qos.prefetchSize = size
		return nil
	}
}

// QosGlobal when global is true, these Qos settings apply to all existing and future
// consumers on all channels on the same connection. When false, the Channel.Qos
// settings will apply to all existing and future consumers on this channel.
// RabbitMQ does not implement the global flag.
func QosGlobal(global bool) Option {
	return func(r *Rabbus) error {
		r.config.qos.global = global
		return nil
	}
}

// Attempts is the max number of retries on broker outages.
func Attempts(attempts int) Option {
	return func(r *Rabbus) error {
		r.config.retryCfg.attempts = attempts
		return nil
	}
}

// Sleep is the sleep time of the retry mechanism.
func Sleep(sleep time.Duration) Option {
	return func(r *Rabbus) error {
		if sleep == 0 {
			r.config.retryCfg.reconnectSleep = time.Second * 10
		}
		r.config.retryCfg.sleep = sleep
		return nil
	}
}

// BreakerInterval is the cyclic period of the closed state for CircuitBreaker to clear the internal counts,
// If Interval is 0, CircuitBreaker doesn't clear the internal counts during the closed state.
func BreakerInterval(interval time.Duration) Option {
	return func(r *Rabbus) error {
		r.config.breaker.interval = interval
		return nil
	}
}

// BreakerTimeout is the period of the open state, after which the state of CircuitBreaker becomes half-open.
// If Timeout is 0, the timeout value of CircuitBreaker is set to 60 seconds.
func BreakerTimeout(timeout time.Duration) Option {
	return func(r *Rabbus) error {
		r.config.breaker.timeout = timeout
		return nil
	}
}

// Threshold when a threshold of failures has been reached, future calls to the broker will not run.
// During this state, the circuit breaker will periodically allow the calls to run and, if it is successful,
// will start running the function again. Default value is 5.
func Threshold(threshold uint32) Option {
	return func(r *Rabbus) error {
		if threshold == 0 {
			threshold = 5
		}
		r.config.breaker.threshold = threshold
		return nil
	}
}

// OnStateChange is called whenever the state of CircuitBreaker changes.
func OnStateChange(fn OnStateChangeFunc) Option {
	return func(r *Rabbus) error {
		r.config.breaker.onStateChange = fn
		return nil
	}
}

// AMQPProvider expose a interface for interacting with amqp broker
func AMQPProvider(provider AMQP) Option {
	return func(r *Rabbus) error {
		if provider != nil {
			r.AMQP = provider
			return nil
		}
		return errors.New("unexpected amqp provider")
	}
}
