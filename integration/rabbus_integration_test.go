package rabbus

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rafaeljesus/rabbus"
)

const (
	amqpDsnEnv = "AMQP_DSN"
)

var (
	amqpDsn string
	timeout = time.After(3 * time.Second)
)

func TestRabbus(t *testing.T) {
	if os.Getenv(amqpDsnEnv) == "" {
		t.SkipNow()
	}

	amqpDsn = os.Getenv(amqpDsnEnv)

	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			"rabbus publish subscribe",
			testRabbusPublishSubscribe,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t)
		})
	}
}

func BenchmarkRabbus(b *testing.B) {
	if os.Getenv(amqpDsnEnv) == "" {
		b.SkipNow()
	}

	tests := []struct {
		scenario string
		function func(*testing.B)
	}{
		{
			"rabbus emit async benchmark",
			benchmarkEmitAsync,
		},
	}

	for _, test := range tests {
		b.Run(test.scenario, func(b *testing.B) {
			test.function(b)
		})
	}
}

func testRabbusPublishSubscribe(t *testing.T) {
	r, err := rabbus.New(
		amqpDsn,
		rabbus.Durable(true),
		rabbus.Attempts(5),
		rabbus.BreakerTimeout(time.Second*2),
	)
	if err != nil {
		t.Fatalf("expected to init rabbus %s", err)
	}

	defer func(r *rabbus.Rabbus) {
		if err = r.Close(); err != nil {
			t.Errorf("expected to close rabbus %s", err)
		}
	}(r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.Run(ctx)

	messages, err := r.Listen(rabbus.ListenConfig{
		Exchange: "test_ex",
		Kind:     rabbus.ExchangeDirect,
		Key:      "test_key",
		Queue:    "test_q",
	})
	if err != nil {
		t.Fatalf("expected to listen message %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func(messages chan rabbus.ConsumerMessage) {
		for m := range messages {
			defer wg.Done()
			close(messages)
			m.Ack(false)
		}
	}(messages)

	msg := rabbus.Message{
		Exchange:     "test_ex",
		Kind:         rabbus.ExchangeDirect,
		Key:          "test_key",
		Payload:      []byte(`foo`),
		DeliveryMode: rabbus.Persistent,
	}

	r.EmitAsync() <- msg

outer:
	for {
		select {
		case <-r.EmitOk():
			wg.Wait()
			break outer
		case <-r.EmitErr():
			t.Fatalf("expected to emit message")
			break outer
		case <-timeout:
			t.Fatalf("parallel.Run() failed, got timeout error")
			break outer
		}
	}
}

func benchmarkEmitAsync(b *testing.B) {
	r, err := rabbus.New(
		amqpDsn,
		rabbus.Attempts(1),
		rabbus.BreakerTimeout(time.Second*2),
	)
	if err != nil {
		b.Fatalf("expected to init rabbus %s", err)
	}

	defer func(r *rabbus.Rabbus) {
		if err := r.Close(); err != nil {
			b.Fatalf("expected to close rabbus %s", err)
		}
	}(r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.Run(ctx)

	var wg sync.WaitGroup
	wg.Add(b.N)

	go func(r *rabbus.Rabbus) {
		for {
			select {
			case _, ok := <-r.EmitOk():
				if ok {
					wg.Done()
				}
			case _, ok := <-r.EmitErr():
				if ok {
					b.Fatalf("expected to emit message, receive error: %v", err)
				}
			}
		}
	}(r)

	for n := 0; n < b.N; n++ {
		msg := rabbus.Message{
			Exchange:     "test_bench_ex" + strconv.Itoa(n%10),
			Kind:         rabbus.ExchangeDirect,
			Key:          "test_key",
			Payload:      []byte(`foo`),
			DeliveryMode: rabbus.Persistent,
		}

		r.EmitAsync() <- msg
	}

	wg.Wait()
}
