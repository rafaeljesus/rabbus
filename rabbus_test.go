package rabbus

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

var (
	mu        sync.RWMutex
	timeout   = time.After(3 * time.Second)
	amqpError = errors.New("amqp error")
)

func TestRabbus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			"validate new rabbus constructor args size",
			testValidateNewRabbusConstructorArgsSize,
		},
		{
			"create new rabbus specifying amqp provider",
			testCreateNewRabbusSpecifyingAmqpProvider,
		},
		{
			"fail to create new rabbus when withQos returns error",
			testFailToCreateNewRabbusWhenWithQosReturnsError,
		},
		{
			"validate rabbus listener",
			testValidateRabbusListener,
		},
		{
			"create new rabbus listener",
			testCreateNewRabbusListener,
		},
		{
			"fail to create new rabbus listener when create consumer returns error",
			testFailToCreateNewRabbusListenerWhenCreateConsumerReturnsError,
		},
		{
			"emit async message",
			testEmitAsyncMessage,
		},
		{
			"emit async message fail to declare exchange",
			testEmitAsyncMessageFailToDeclareExchange,
		},
		{
			"emit async message fail to publish",
			testEmitAsyncMessageFailToPublish,
		},
		{
			"emit async message ensure breaker",
			testEmitAsyncMessageEnsureBreaker,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t)
		})
	}
}

func testValidateNewRabbusConstructorArgsSize(t *testing.T) {
	amqpProvider := new(amqpMock)
	amqpProvider.withQosFn = func(count, size int, global bool) error { return nil }
	r, err := NewRabbus(Config{}, amqpProvider, amqpProvider)
	if r != nil {
		t.Fatal("unexpected rabbus value")
	}

	if err != ErrUnsupportedArguments {
		t.Fatalf("expected to have ErrUnsupportedArguments, got %s", err)
	}

	if amqpProvider.withQosInvoked {
		t.Fatal("unexpected call to withQos func")
	}
}

func testCreateNewRabbusSpecifyingAmqpProvider(t *testing.T) {
	config := Config{
		Qos: Qos{
			PrefetchCount: 1,
			PrefetchSize:  10,
			Global:        true,
		},
	}
	amqpProvider := new(amqpMock)
	amqpProvider.withQosFn = func(count, size int, global bool) error {
		if config.Qos.PrefetchCount != count {
			t.Fatalf("unexpected prefetch count: %t", count)
		}
		if config.Qos.PrefetchSize != size {
			t.Fatalf("unexpected prefetch size: %t", size)
		}
		if config.Qos.Global != global {
			t.Fatalf("unexpected global: %t", global)
		}
		return nil
	}
	r, err := NewRabbus(config, amqpProvider)
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err)
	}

	defer func(r *Rabbus) {
		if err := r.Close(); err != nil {
			t.Errorf("expected to close rabbus %s", err)
		}
	}(r)

	if !amqpProvider.withQosInvoked {
		t.Fatal("expected amqpProvider.WithQos() to be invoked")
	}
}

func testFailToCreateNewRabbusWhenWithQosReturnsError(t *testing.T) {
	amqpProvider := new(amqpMock)
	amqpProvider.withQosFn = func(count, size int, global bool) error { return amqpError }
	r, err := NewRabbus(Config{}, amqpProvider)
	if r != nil {
		t.Fatal("unexpected rabbus value")
	}

	if err != amqpError {
		t.Fatalf("expected to have error %v, got %v", amqpError, err)
	}
}

func testValidateRabbusListener(t *testing.T) {
	amqpProvider := new(amqpMock)
	amqpProvider.withQosFn = func(count, size int, global bool) error { return nil }
	r, err := NewRabbus(Config{}, amqpProvider)
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err)
	}

	defer func(r *Rabbus) {
		if err := r.Close(); err != nil {
			t.Errorf("expected to close rabbus %s", err)
		}
	}(r)

	configs := []struct {
		config ListenConfig
		errMsg string
	}{
		{
			ListenConfig{},
			"expected to validate exchange",
		},
		{
			ListenConfig{Exchange: "ex"},
			"expected to validate kind",
		},
		{
			ListenConfig{Exchange: "ex", Kind: "topic"},
			"expected to validate queue",
		},
	}

	for _, c := range configs {
		_, err := r.Listen(c.config)
		if err == nil {
			t.Fatal(c.errMsg)
		}
	}
}

func testCreateNewRabbusListener(t *testing.T) {
	c := Config{Durable: true}
	config := ListenConfig{
		Exchange: "exchange",
		Kind:     "direct",
		Key:      "key",
		Queue:    "queue",
	}
	amqpProvider := new(amqpMock)
	amqpProvider.withQosFn = func(count, size int, global bool) error { return nil }
	amqpProvider.createConsumerFn = func(exchange, key, kind, queue string, durable bool) (<-chan amqp.Delivery, error) {
		if exchange != config.Exchange {
			t.Fatalf("unexpected exchange: %s", exchange)
		}
		if key != config.Key {
			t.Fatalf("unexpected key: %s", key)
		}
		if kind != config.Kind {
			t.Fatalf("unexpected kind: %s", kind)
		}
		if queue != config.Queue {
			t.Fatalf("unexpected queue: %s", queue)
		}
		if durable != c.Durable {
			t.Fatalf("unexpected durable: %t", durable)
		}
		return make(<-chan amqp.Delivery), nil
	}
	r, err := NewRabbus(c, amqpProvider)
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err)
	}

	defer func(r *Rabbus) {
		if err := r.Close(); err != nil {
			t.Errorf("expected to close rabbus %s", err)
		}
	}(r)

	if _, err = r.Listen(config); err != nil {
		t.Fatalf("expected to create listener, got %s", err)
	}

	if !amqpProvider.createConsumerInvoked {
		t.Fatal("expected amqpProvider.CreateConsumer() to be invoked")
	}
}

func testFailToCreateNewRabbusListenerWhenCreateConsumerReturnsError(t *testing.T) {
	amqpProvider := new(amqpMock)
	amqpProvider.withQosFn = func(count, size int, global bool) error { return nil }
	amqpProvider.createConsumerFn = func(exchange, key, kind, queue string, durable bool) (<-chan amqp.Delivery, error) {
		return nil, amqpError
	}
	r, err := NewRabbus(Config{}, amqpProvider)
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err.Error())
	}

	_, err = r.Listen(ListenConfig{
		Exchange: "exchange",
		Kind:     "direct",
		Key:      "key",
		Queue:    "queue",
	})
	if err != amqpError {
		t.Fatalf("expected to have error %v, got %v", amqpError, err)
	}
}

func testEmitAsyncMessage(t *testing.T) {
	msg := Message{
		Exchange: "exchange",
		Kind:     "direct",
		Key:      "key",
		Payload:  []byte(`foo`),
	}
	c := Config{Durable: true}
	amqpProvider := new(amqpMock)
	amqpProvider.withQosFn = func(count, size int, global bool) error { return nil }
	amqpProvider.withExchangeFn = func(exchange, kind string, durable bool) error {
		if exchange != msg.Exchange {
			t.Fatalf("unexpected exchange: %s", exchange)
		}
		if kind != msg.Kind {
			t.Fatalf("unexpected kind: %s", kind)
		}
		if durable != c.Durable {
			t.Fatalf("unexpected durable: %t", durable)
		}
		return nil
	}
	amqpProvider.publishFn = func(exchange, key string, opts amqp.Publishing) error {
		if exchange != msg.Exchange {
			t.Fatalf("unexpected exchange: %s", exchange)
		}
		if key != msg.Key {
			t.Fatalf("unexpected key: %s", key)
		}
		if string(opts.Body) != string(msg.Payload) {
			t.Fatalf("unexpected payload: %s", string(opts.Body))
		}
		return nil
	}
	r, err := NewRabbus(c, amqpProvider)
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err)
	}

	defer func(r *Rabbus) {
		if err := r.Close(); err != nil {
			t.Fatalf("expected to close rabbus %s", err)
		}
	}(r)

	r.EmitAsync() <- msg

outer:
	for {
		select {
		case <-r.EmitOk():
			if !amqpProvider.publishInvoked {
				t.Fatal("expected amqpProvider.Publish() to be invoked")
			}
			break outer
		case err := <-r.EmitErr():
			t.Fatalf("expected to emit message %v: ", err)
			break outer
		case <-timeout:
			t.Fatalf("got timeout error during emit async")
			break outer
		}
	}
}

func testEmitAsyncMessageFailToDeclareExchange(t *testing.T) {
	msg := Message{
		Exchange: "exchange",
		Kind:     "direct",
		Key:      "key",
		Payload:  []byte(`foo`),
	}
	amqpProvider := new(amqpMock)
	amqpProvider.withQosFn = func(count, size int, global bool) error { return nil }
	amqpProvider.withExchangeFn = func(exchange, kind string, durable bool) error { return amqpError }
	amqpProvider.publishFn = func(exchange, key string, opts amqp.Publishing) error { return nil }
	r, err := NewRabbus(Config{}, amqpProvider)
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err)
	}

	defer func(r *Rabbus) {
		if err := r.Close(); err != nil {
			t.Fatalf("expected to close rabbus %s", err)
		}
	}(r)

	r.EmitAsync() <- msg

outer:
	for {
		select {
		case err := <-r.EmitErr():
			if err != amqpError {
				t.Fatalf("expected to have error %v, got %v", amqpError, err)
			}
			if amqpProvider.publishInvoked {
				t.Fatal("expected amqpProvider.Publish() to not be invoked")
			}
			break outer
		case <-timeout:
			t.Errorf("Got timeout error during emit async")
			break outer
		}
	}
}

func testEmitAsyncMessageFailToPublish(t *testing.T) {
	msg := Message{
		Exchange: "exchange",
		Kind:     "direct",
		Key:      "key",
		Payload:  []byte(`foo`),
	}
	amqpProvider := new(amqpMock)
	amqpProvider.withQosFn = func(count, size int, global bool) error { return nil }
	amqpProvider.withExchangeFn = func(exchange, kind string, durable bool) error { return nil }
	amqpProvider.publishFn = func(exchange, key string, opts amqp.Publishing) error { return amqpError }
	r, err := NewRabbus(Config{}, amqpProvider)
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err)
	}

	defer func(r *Rabbus) {
		if err := r.Close(); err != nil {
			t.Errorf("expected to close rabbus %s", err)
		}
	}(r)

	r.EmitAsync() <- msg

outer:
	for {
		select {
		case err := <-r.EmitErr():
			if err != amqpError {
				t.Fatalf("expected to have error %v, got %v", amqpError, err)
			}
			break outer
		case <-timeout:
			t.Errorf("Got timeout error during emit async")
			break outer
		}
	}
}

func testEmitAsyncMessageEnsureBreaker(t *testing.T) {
	var breakerCalled bool
	c := Config{
		Breaker: Breaker{
			Threshold: 1,
			OnStateChange: func(name, from, to string) {
				breakerCalled = true
			},
		},
	}
	msg := Message{
		Exchange: "exchange",
		Kind:     "direct",
		Key:      "key",
		Payload:  []byte(`foo`),
	}
	amqpProvider := new(amqpMock)
	amqpProvider.withQosFn = func(count, size int, global bool) error { return nil }
	amqpProvider.withExchangeFn = func(exchange, kind string, durable bool) error { return nil }
	amqpProvider.publishFn = func(exchange, key string, opts amqp.Publishing) error { return amqpError }
	r, err := NewRabbus(c, amqpProvider)
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err)
	}

	defer func(r *Rabbus) {
		if err := r.Close(); err != nil {
			t.Errorf("expected to close rabbus %s", err)
		}
	}(r)

	r.EmitAsync() <- msg

	count := 0
outer:
	for {
		select {
		case <-r.EmitErr():
			count++
			if count == 2 {
				if !breakerCalled {
					t.Fatal("expected config.Breaker.OnStateChange to be invoked")
				}
				break outer
			}
			r.EmitAsync() <- msg
		case <-timeout:
			t.Fatal("got timeout error during emit async")
			break outer
		}
	}
}

type amqpMock struct {
	publishInvoked        bool
	publishFn             func(exchange, key string, opts amqp.Publishing) error
	createConsumerInvoked bool
	createConsumerFn      func(exchange, key, kind, queue string, durable bool) (<-chan amqp.Delivery, error)
	withExchangeInvoked   bool
	withExchangeFn        func(exchange, kind string, durable bool) error
	withQosInvoked        bool
	withQosFn             func(count, size int, global bool) error
}

func (m *amqpMock) Publish(exchange, key string, opts amqp.Publishing) error {
	m.publishInvoked = true
	return m.publishFn(exchange, key, opts)
}

func (m *amqpMock) CreateConsumer(exchange, key, kind, queue string, durable bool) (<-chan amqp.Delivery, error) {
	m.createConsumerInvoked = true
	return m.createConsumerFn(exchange, key, kind, queue, durable)
}

func (m *amqpMock) WithExchange(exchange, kind string, durable bool) error {
	m.withExchangeInvoked = true
	return m.withExchangeFn(exchange, kind, durable)
}

func (m *amqpMock) WithQos(count, size int, global bool) error {
	m.withQosInvoked = true
	return m.withQosFn(count, size, global)
}

func (m *amqpMock) NotifyClose(c chan *amqp.Error) chan *amqp.Error { return nil }
func (m *amqpMock) Close() error                                    { return nil }
