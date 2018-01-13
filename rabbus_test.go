package rabbus

import (
	"testing"
	"time"

	"github.com/streadway/amqp"
)

const (
	RABBUS_DSN = "amqp://localhost:5672"
)

var (
	timeout = time.After(3 * time.Second)
)

func TestRabbus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			scenario: "validate new rabbus constructor args size",
			function: testValidateNewRabbusConstructorArgsSize,
		},
		{
			scenario: "create new rabbus specifying amqp provider",
			function: testCreateNewRabbusSpecifyingAmqpProvider,
		},
		{
			scenario: "validate rabbus listener",
			function: testValidateRabbusListener,
		},
		{
			scenario: "create new rabbus listener",
			function: testCreateNewRabbusListener,
		},
		{
			scenario: "emit async message",
			function: testEmitAsyncMessage,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t)
		})
	}
}

func testValidateNewRabbusConstructorArgsSize(t *testing.T) {
	amqpWrapper := newAmqpMock()
	r, err := NewRabbus(Config{}, amqpWrapper, amqpWrapper)
	if r != nil {
		t.Error("Expected to not create new rabbus")
	}

	if err != ErrUnsupportedArguments {
		t.Errorf("Expected to have ErrUnsupportedArguments, got %s", err)
	}
}

func testCreateNewRabbusSpecifyingAmqpProvider(t *testing.T) {
	amqpWrapper := newAmqpMock()
	config := Config{
		Qos: Qos{
			PrefetchCount: 1,
			PrefetchSize:  10,
			Global:        true,
		},
	}
	r, err := NewRabbus(config, amqpWrapper)
	if err != nil {
		t.Errorf("Expected to create new rabbus, got %s", err)
	}

	defer func(r Rabbus) {
		if err := r.Close(); err != nil {
			t.Errorf("Expected to close rabbus %s", err)
		}
	}(r)

	qosPrefetchCount, ok := amqpWrapper.withQosCaller["count"]
	if !ok {
		t.Error("Expected to have called withQos prefetch count")
	}

	if qosPrefetchCount != config.Qos.PrefetchCount {
		t.Errorf("Expected to have called withQos prefetch count with %v, got %v", config.Qos.PrefetchCount, qosPrefetchCount)
	}

	qosPrefetchSize, ok := amqpWrapper.withQosCaller["size"]
	if !ok {
		t.Error("Expected to have called withQos prefetch size")
	}

	if qosPrefetchSize != config.Qos.PrefetchSize {
		t.Errorf("Expected to have called withQos prefetch size with %v, got %v", config.Qos.PrefetchSize, qosPrefetchSize)
	}

	qosGlobal, ok := amqpWrapper.withQosCaller["global"]
	if !ok {
		t.Error("Expected to have called withQos prefetch size")
	}

	if qosGlobal != config.Qos.Global {
		t.Errorf("Expected to have called withQos global with %v, got %v", config.Qos.Global, qosGlobal)
	}
}

func testValidateRabbusListener(t *testing.T) {
	amqpWrapper := newAmqpMock()
	r, err := NewRabbus(Config{}, amqpWrapper)
	if err != nil {
		t.Error("Expected to not create new rabbus")
	}

	defer func(r Rabbus) {
		if err := r.Close(); err != nil {
			t.Errorf("Expected to close rabbus %s", err)
		}
	}(r)

	configs := []struct {
		config ListenConfig
		errMsg string
	}{
		{
			config: ListenConfig{},
			errMsg: "Expected to validate exchange",
		},
		{
			config: ListenConfig{Exchange: "ex"},
			errMsg: "Expected to validate kind",
		},
		{
			config: ListenConfig{Exchange: "ex", Kind: "topic"},
			errMsg: "Expected to validate queue",
		},
	}

	for _, c := range configs {
		_, err := r.Listen(c.config)
		if err == nil {
			t.Errorf(c.errMsg)
		}
	}
}

func testCreateNewRabbusListener(t *testing.T) {
	amqpWrapper := newAmqpMock()
	c := Config{Durable: true}
	r, err := NewRabbus(c, amqpWrapper)
	if err != nil {
		t.Error("Expected to not create new rabbus")
	}

	defer func(r Rabbus) {
		if err := r.Close(); err != nil {
			t.Errorf("Expected to close rabbus %s", err)
		}
	}(r)

	config := ListenConfig{
		Exchange: "exchange",
		Kind:     "direct",
		Key:      "key",
		Queue:    "queue",
	}

	if _, err = r.Listen(config); err != nil {
		t.Errorf("Expected to create listener, got %s", err)
	}

	exchange, ok := amqpWrapper.createConsumerCaller["exchange"]
	if !ok {
		t.Error("Expected to have called createConsumer with exchange value")
	}

	if exchange != config.Exchange {
		t.Errorf("Expected to have called createConsumer exchange with %v, got %v", config.Exchange, exchange)
	}

	kind, ok := amqpWrapper.createConsumerCaller["kind"]
	if !ok {
		t.Error("Expected to have called createConsumer with kind value")
	}

	if kind != config.Kind {
		t.Errorf("Expected to have called createConsumer kind with %v, got %v", config.Kind, kind)
	}

	queue, ok := amqpWrapper.createConsumerCaller["queue"]
	if !ok {
		t.Error("Expected to have called createConsumer with queue value")
	}

	if queue != config.Queue {
		t.Errorf("Expected to have called createConsumer queue with %v, got %v", config.Queue, queue)
	}

	durable, ok := amqpWrapper.createConsumerCaller["durable"]
	if !ok {
		t.Error("Expected to have called createConsumer with durable value")
	}

	if durable != c.Durable {
		t.Errorf("Expected to have called createConsumer durable with %v, got %v", c.Durable, durable)
	}
}

func testEmitAsyncMessage(t *testing.T) {
	amqpWrapper := newAmqpMock()
	c := Config{Durable: true}
	r, err := NewRabbus(c, amqpWrapper)
	if err != nil {
		t.Error("Expected to not create new rabbus")
	}

	defer func(r Rabbus) {
		if err := r.Close(); err != nil {
			t.Errorf("Expected to close rabbus %s", err)
		}
	}(r)

	msg := Message{
		Exchange: "exchange",
		Kind:     "direct",
		Key:      "key",
		Payload:  []byte(`foo`),
	}

	r.EmitAsync() <- msg

outer:
	for {
		select {
		case <-r.EmitOk():
			exchange, ok := amqpWrapper.withExchangeCaller["exchange"]
			if !ok {
				t.Error("Expected to have called withExchange with exchange value")
			}

			if exchange != msg.Exchange {
				t.Errorf("Expected to have called withExchange exchange with %v, got %v", msg.Exchange, exchange)
			}

			kind, ok := amqpWrapper.withExchangeCaller["kind"]
			if !ok {
				t.Error("Expected to have called withExchange with kind value")
			}

			if kind != msg.Kind {
				t.Errorf("Expected to have called withExchange kind with %v, got %v", msg.Kind, kind)
			}

			durable, ok := amqpWrapper.withExchangeCaller["durable"]
			if !ok {
				t.Error("Expected to have called withExchange with durable value")
			}

			if durable != c.Durable {
				t.Errorf("Expected to have called withExchange durable with %v, got %v", c.Durable, durable)
			}

			exchange, ok = amqpWrapper.publishCaller["exchange"]
			if !ok {
				t.Error("Expected to have called publish with exchange value")
			}

			key, ok := amqpWrapper.publishCaller["key"]
			if !ok {
				t.Error("Expected to have called publish with key value")
			}

			if key != msg.Key {
				t.Errorf("Expected to have called publish key with %v, got %v", msg.Key, key)
			}

			opts, ok := amqpWrapper.publishCaller["opts"]
			if !ok {
				t.Error("Expected to have called publish with opts value")
			}

			o := opts.(amqp.Publishing)
			payload := string(o.Body)
			expectedPayload := string(msg.Payload)
			if payload != expectedPayload {
				t.Errorf("Expected to have called publish payload with %v, got %v", expectedPayload, payload)
			}

			break outer
		case <-r.EmitErr():
			t.Errorf("Expected to emit message")
			break outer
		case <-timeout:
			t.Errorf("Got timeout error during emit async")
			break outer
		}
	}
}

type amqpMock struct {
	publishCaller        map[string]interface{}
	createConsumerCaller map[string]interface{}
	withExchangeCaller   map[string]interface{}
	withQosCaller        map[string]interface{}
}

func newAmqpMock() *amqpMock {
	return &amqpMock{
		publishCaller:        make(map[string]interface{}),
		createConsumerCaller: make(map[string]interface{}),
		withExchangeCaller:   make(map[string]interface{}),
		withQosCaller:        make(map[string]interface{}),
	}
}

func (m *amqpMock) Publish(exchange, key string, opts amqp.Publishing) error {
	m.publishCaller["exchange"] = exchange
	m.publishCaller["key"] = key
	m.publishCaller["opts"] = opts
	return nil
}

func (m *amqpMock) CreateConsumer(exchange, key, kind, queue string, durable bool) (<-chan amqp.Delivery, error) {
	m.createConsumerCaller["exchange"] = exchange
	m.createConsumerCaller["key"] = key
	m.createConsumerCaller["kind"] = kind
	m.createConsumerCaller["queue"] = queue
	m.createConsumerCaller["durable"] = durable
	return make(<-chan amqp.Delivery), nil
}

func (m *amqpMock) WithExchange(exchange, kind string, durable bool) error {
	m.withExchangeCaller["exchange"] = exchange
	m.withExchangeCaller["kind"] = kind
	m.withExchangeCaller["durable"] = durable
	return nil
}

func (m *amqpMock) WithQos(count, size int, global bool) error {
	m.withQosCaller["count"] = count
	m.withQosCaller["size"] = size
	m.withQosCaller["global"] = global
	return nil
}

func (m *amqpMock) NotifyClose(c chan *amqp.Error) chan *amqp.Error { return nil }
func (m *amqpMock) Close() error                                    { return nil }
