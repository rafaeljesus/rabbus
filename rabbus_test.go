package rabbus

import (
	"strconv"
	"sync"
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
			scenario: "rabbus listen",
			function: testRabbusListen,
		},
		{
			scenario: "rabbus with managed connection listen",
			function: testRabbusWithManagedConnListen,
		},
		{
			scenario: "rabbus listen validate",
			function: testRabbusListenValidate,
		},
		{
			scenario: "rabbus close",
			function: testRabbusClose,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t)
		})
	}
}

func BenchmarkRabbus(b *testing.B) {
	tests := []struct {
		scenario string
		function func(*testing.B)
	}{
		{
			scenario: "rabbus emit async benchmark",
			function: benchmarkEmitAsync,
		},
	}

	for _, test := range tests {
		b.Run(test.scenario, func(b *testing.B) {
			test.function(b)
		})
	}
}

func testRabbusListen(t *testing.T) {
	r, err := NewRabbus(Config{
		Dsn:     RABBUS_DSN,
		Durable: true,
		Retry: Retry{
			Attempts: 1,
		},
		Breaker: Breaker{
			Timeout: time.Second * 2,
		},
	})
	if err != nil {
		t.Errorf("Expected to init rabbus %s", err)
	}

	defer func(r Rabbus) {
		if err = r.Close(); err != nil {
			t.Errorf("Expected to close rabbus %s", err)
		}
	}(r)

	messages, err := r.Listen(ListenConfig{
		Exchange: "test_ex",
		Kind:     "direct",
		Key:      "test_key",
		Queue:    "test_q",
	})
	if err != nil {
		t.Errorf("Expected to listen message %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func(messages chan ConsumerMessage) {
		for m := range messages {
			m.Ack(false)
			close(messages)
			wg.Done()
		}
	}(messages)

	msg := Message{
		Exchange:     "test_ex",
		Kind:         "direct",
		Key:          "test_key",
		Payload:      []byte(`foo`),
		DeliveryMode: Persistent,
	}

	r.EmitAsync() <- msg

outer:
	for {
		select {
		case <-r.EmitOk():
			wg.Wait()
			break outer
		case <-r.EmitErr():
			t.Errorf("Expected to emit message")
			break outer
		case <-timeout:
			t.Errorf("parallel.Run() failed, got timeout error")
			break outer
		}
	}
}

func testRabbusWithManagedConnListen(t *testing.T) {
	conn, err := amqp.Dial(RABBUS_DSN)
	if err != nil {
		t.Errorf("Expected to create amqp.Connection %s", err)
	}

	r, err := NewRabbusWithManagedConn(conn, Config{
		Dsn:     RABBUS_DSN,
		Durable: true,
		Retry: Retry{
			Attempts: 1,
		},
		Breaker: Breaker{
			Timeout: time.Second * 2,
		},
	})
	if err != nil {
		t.Errorf("Expected to init rabbus %s", err)
	}

	defer func(r Rabbus) {
		if err = r.Close(); err != nil {
			t.Errorf("Expected to close rabbus %s", err)
		}
	}(r)

	messages, err := r.Listen(ListenConfig{
		Exchange: "test_ex",
		Kind:     "direct",
		Key:      "test_key",
		Queue:    "test_q",
	})
	if err != nil {
		t.Errorf("Expected to listen message %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func(messages chan ConsumerMessage) {
		for m := range messages {
			m.Ack(false)
			close(messages)
			wg.Done()
		}
	}(messages)

	msg := Message{
		Exchange:     "test_ex",
		Kind:         "direct",
		Key:          "test_key",
		Payload:      []byte(`foo`),
		DeliveryMode: Persistent,
	}

	r.EmitAsync() <- msg

outer:
	for {
		select {
		case <-r.EmitOk():
			wg.Wait()
			break outer
		case <-r.EmitErr():
			t.Errorf("Expected to emit message")
			break outer
		case <-timeout:
			t.Errorf("parallel.Run() failed, got timeout error")
			break outer
		}
	}
}

func testRabbusListenValidate(t *testing.T) {
	r, err := NewRabbus(Config{
		Dsn: RABBUS_DSN,
		Retry: Retry{
			Attempts: 1,
		},
		Breaker: Breaker{
			Timeout: time.Second * 2,
		},
	})
	if err != nil {
		t.Errorf("Expected to init rabbus %s", err)
	}

	defer func(r Rabbus) {
		if err = r.Close(); err != nil {
			t.Errorf("Expected to close rabbus %s", err)
		}
	}(r)

	configs := []struct {
		config ListenConfig
		errMsg string
	}{
		{
			config: ListenConfig{},
			errMsg: "Expected to validate Exchange",
		},
		{
			config: ListenConfig{Exchange: "foo"},
			errMsg: "Expected to validate Kind",
		},
		{
			config: ListenConfig{Exchange: "foo", Kind: "direct"},
			errMsg: "Expected to validate Queue",
		},
	}

	for _, c := range configs {
		_, err := r.Listen(c.config)
		if err == nil {
			t.Errorf(c.errMsg)
		}
	}
}

func testRabbusClose(t *testing.T) {
	r, err := NewRabbus(Config{
		Dsn: RABBUS_DSN,
		Retry: Retry{
			Attempts: 1,
		},
		Breaker: Breaker{
			Timeout: time.Second * 2,
		},
	})
	if err != nil {
		t.Errorf("Expected to init rabbus %s", err)
	}

	if err = r.Close(); err != nil {
		t.Errorf("Expected to close rabbus %s", err)
	}
}

func benchmarkEmitAsync(b *testing.B) {
	r, err := NewRabbus(Config{
		Dsn:     RABBUS_DSN,
		Durable: false,
		Retry: Retry{
			Attempts: 1,
		},
		Breaker: Breaker{
			Timeout: time.Second * 2,
		},
	})
	if err != nil {
		b.Errorf("Expected to init rabbus %s", err)
	}

	defer func(r Rabbus) {
		if err = r.Close(); err != nil {
			b.Errorf("Expected to close rabbus %s", err)
		}
	}(r)

	var wg sync.WaitGroup
	wg.Add(b.N)

	go func(r Rabbus) {
		for {
			select {
			case <-r.EmitOk():
				wg.Done()
			case err := <-r.EmitErr():
				b.Fatalf("Expected to emit message, receive error: %v", err)
			}
		}
	}(r)

	for n := 0; n < b.N; n++ {
		ex := "test_bench_ex" + strconv.Itoa(n%10)
		r.EmitAsync() <- Message{
			Exchange:     ex,
			Kind:         "direct",
			Key:          "test_key",
			Payload:      []byte(`foo`),
			DeliveryMode: Persistent,
		}
	}
	wg.Wait()
}
