package rabbus

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

var (
	dsn     = "amqp://path"
	mu      sync.RWMutex
	timeout = time.After(3 * time.Second)
	errAmqp = errors.New("amqp error")
)

func TestRabbus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			"create new rabbus specifying amqp provider",
			testCreateNewSpecifyingAmqpProvider,
		},
		{
			"fail to create new rabbus when withQos returns error",
			testFailToCreateNewWhenWithQosReturnsError,
		},
		{
			"validate rabbus listener",
			testValidateRabbusListener,
		},
		{
			"create new rabbus listener",
			testCreateNewListener,
		},
		{
			"fail to create new rabbus listener when create consumer returns error",
			testFailToCreateNewListenerWhenCreateConsumerReturnsError,
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

func testCreateNewSpecifyingAmqpProvider(t *testing.T) {
	count := 1
	size := 10
	global := true
	provider := new(amqpMock)
	provider.withQosFn = func(c, s int, g bool) error {
		if count != c {
			t.Fatalf("unexpected prefetch count: %d", c)
		}
		if size != s {
			t.Fatalf("unexpected prefetch size: %d", s)
		}
		if global != g {
			t.Fatalf("unexpected global: %t", g)
		}
		return nil
	}
	_, err := New(dsn, PrefetchCount(count), PrefetchSize(size), QosGlobal(global), AmqpProvider(provider))
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err)
	}

	if !provider.withQosInvoked {
		t.Fatal("expected provider.WithQos() to be invoked")
	}
}

func testFailToCreateNewWhenWithQosReturnsError(t *testing.T) {
	provider := new(amqpMock)
	provider.withQosFn = func(count, size int, global bool) error { return errAmqp }
	r, err := New(dsn, AmqpProvider(provider))
	if r != nil {
		t.Fatal("unexpected rabbus value")
	}

	if err != errAmqp {
		t.Fatalf("expected to have error %v, got %v", errAmqp, err)
	}
}

func testValidateRabbusListener(t *testing.T) {
	provider := new(amqpMock)
	provider.withQosFn = func(count, size int, global bool) error { return nil }
	r, err := New(dsn, AmqpProvider(provider))
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

func testCreateNewListener(t *testing.T) {
	durable := true
	config := ListenConfig{
		Exchange: "exchange",
		Kind:     ExchangeDirect,
		Key:      "key",
		Queue:    "queue",
	}
	provider := new(amqpMock)
	provider.withQosFn = func(count, size int, global bool) error { return nil }
	provider.createConsumerFn = func(exchange, key, kind, queue string, d bool) (<-chan amqp.Delivery, error) {
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
		if d != durable {
			t.Fatalf("unexpected durable: %t", d)
		}
		return make(<-chan amqp.Delivery), nil
	}
	r, err := New(dsn, AmqpProvider(provider), Durable(durable))
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

	if !provider.createConsumerInvoked {
		t.Fatal("expected provider.CreateConsumer() to be invoked")
	}
}

func testFailToCreateNewListenerWhenCreateConsumerReturnsError(t *testing.T) {
	provider := new(amqpMock)
	provider.withQosFn = func(count, size int, global bool) error { return nil }
	provider.createConsumerFn = func(exchange, key, kind, queue string, durable bool) (<-chan amqp.Delivery, error) {
		return nil, errAmqp
	}
	r, err := New(dsn, AmqpProvider(provider))
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err.Error())
	}

	_, err = r.Listen(ListenConfig{
		Exchange: "exchange",
		Kind:     ExchangeDirect,
		Key:      "key",
		Queue:    "queue",
	})
	if err != errAmqp {
		t.Fatalf("expected to have error %v, got %v", errAmqp, err)
	}
}

func testEmitAsyncMessage(t *testing.T) {
	msg := Message{
		Exchange: "exchange",
		Kind:     ExchangeDirect,
		Key:      "key",
		Payload:  []byte(`foo`),
	}
	provider := new(amqpMock)
	provider.withQosFn = func(count, size int, global bool) error { return nil }
	provider.withExchangeFn = func(exchange, kind string, durable bool) error {
		if exchange != msg.Exchange {
			t.Fatalf("unexpected exchange: %s", exchange)
		}
		if kind != msg.Kind {
			t.Fatalf("unexpected kind: %s", kind)
		}
		if durable {
			t.Fatalf("unexpected durable: %t", durable)
		}
		return nil
	}
	provider.publishFn = func(exchange, key string, opts amqp.Publishing) error {
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
	r, err := New(dsn, AmqpProvider(provider))
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err)
	}

	defer func(r *Rabbus) {
		if err := r.Close(); err != nil {
			t.Fatalf("expected to close rabbus %s", err)
		}
	}(r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.Run(ctx)

	r.EmitAsync() <- msg

outer:
	for {
		select {
		case <-r.EmitOk():
			if !provider.publishInvoked {
				t.Fatal("expected provider.Publish() to be invoked")
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
		Kind:     ExchangeDirect,
		Key:      "key",
		Payload:  []byte(`foo`),
	}
	provider := new(amqpMock)
	provider.withQosFn = func(count, size int, global bool) error { return nil }
	provider.withExchangeFn = func(exchange, kind string, durable bool) error { return errAmqp }
	provider.publishFn = func(exchange, key string, opts amqp.Publishing) error { return nil }
	r, err := New(dsn, AmqpProvider(provider))
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err)
	}

	defer func(r *Rabbus) {
		if err := r.Close(); err != nil {
			t.Fatalf("expected to close rabbus %s", err)
		}
	}(r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.Run(ctx)

	r.EmitAsync() <- msg
outer:
	for {
		select {
		case err := <-r.EmitErr():
			if err != errAmqp {
				t.Fatalf("expected to have error %v, got %v", errAmqp, err)
			}
			if provider.publishInvoked {
				t.Fatal("expected provider.Publish() to not be invoked")
			}
			break outer
		case <-timeout:
			t.Errorf("got timeout error during emit async")
			break outer
		}
	}
}

func testEmitAsyncMessageFailToPublish(t *testing.T) {
	msg := Message{
		Exchange: "exchange",
		Kind:     ExchangeDirect,
		Key:      "key",
		Payload:  []byte(`foo`),
	}
	provider := new(amqpMock)
	provider.withQosFn = func(count, size int, global bool) error { return nil }
	provider.withExchangeFn = func(exchange, kind string, durable bool) error { return nil }
	provider.publishFn = func(exchange, key string, opts amqp.Publishing) error { return errAmqp }
	r, err := New(dsn, AmqpProvider(provider))
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err)
	}

	defer func(r *Rabbus) {
		if err := r.Close(); err != nil {
			t.Errorf("expected to close rabbus %s", err)
		}
	}(r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.Run(ctx)

	r.EmitAsync() <- msg

outer:
	for {
		select {
		case err := <-r.EmitErr():
			if err != errAmqp {
				t.Fatalf("expected to have error %v, got %v", errAmqp, err)
			}
			break outer
		case <-timeout:
			t.Errorf("got timeout error during emit async")
			break outer
		}
	}
}

func testEmitAsyncMessageEnsureBreaker(t *testing.T) {
	var breakerCalled bool
	fn := func(name, from, to string) { breakerCalled = true }
	threshold := uint32(1)
	msg := Message{
		Exchange: "exchange",
		Kind:     ExchangeDirect,
		Key:      "key",
		Payload:  []byte(`foo`),
	}
	provider := new(amqpMock)
	provider.withQosFn = func(count, size int, global bool) error { return nil }
	provider.withExchangeFn = func(exchange, kind string, durable bool) error { return nil }
	provider.publishFn = func(exchange, key string, opts amqp.Publishing) error { return errAmqp }
	r, err := New(dsn, AmqpProvider(provider), OnStateChange(fn), Threshold(threshold))
	if err != nil {
		t.Fatalf("expected to create new rabbus, got %s", err)
	}

	defer func(r *Rabbus) {
		if err := r.Close(); err != nil {
			t.Errorf("expected to close rabbus %s", err)
		}
	}(r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.Run(ctx)

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
