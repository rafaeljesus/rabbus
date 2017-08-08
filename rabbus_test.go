package rabbus

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

var RABBUS_DSN = "amqp://localhost:5672"

func TestRabbusListen(t *testing.T) {
	r, err := NewRabbus(Config{
		Dsn:      RABBUS_DSN,
		Attempts: 1,
		Timeout:  time.Second * 2,
		Durable:  true,
	})
	if err != nil {
		t.Errorf("Expected to init rabbus %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	messages, err := r.Listen(ListenConfig{
		Exchange: "test_ex",
		Kind:     "direct",
		Key:      "test_key",
		Queue:    "test_q",
	})
	if err != nil {
		t.Errorf("Expected to listen message %s", err)
	}

	go func() {
		for m := range messages {
			m.Ack(false)
			wg.Done()
		}
	}()

	r.EmitAsync() <- Message{
		Exchange:     "test_ex",
		Kind:         "direct",
		Key:          "test_key",
		Payload:      []byte(`foo`),
		DeliveryMode: Persistent,
	}

	go func() {
		for {
			select {
			case <-r.EmitOk():
			case <-r.EmitErr():
				t.Errorf("Expected to emit message")
				wg.Done()
			}
		}
	}()

	wg.Wait()
}

func TestRabbusListen_Validate(t *testing.T) {
	r, err := NewRabbus(Config{
		Dsn:      RABBUS_DSN,
		Attempts: 1,
		Timeout:  time.Second * 2,
	})
	if err != nil {
		t.Errorf("Expected to init rabbus %s", err)
	}

	_, err = r.Listen(ListenConfig{})
	if err == nil {
		t.Errorf("Expected to validate Exchange")
	}

	_, err = r.Listen(ListenConfig{Exchange: "foo"})
	if err == nil {
		t.Errorf("Expected to validate Kind")
	}

	_, err = r.Listen(ListenConfig{
		Exchange: "foo2",
		Kind:     "direct",
	})
	if err == nil {
		t.Errorf("Expected to validate Queue")
	}
}

func TestRabbusClose(t *testing.T) {
	r, err := NewRabbus(Config{
		Dsn:      RABBUS_DSN,
		Attempts: 1,
		Timeout:  time.Second * 2,
	})
	if err != nil {
		t.Errorf("Expected to init rabbus %s", err)
	}

	r.Close()
}

func BenchmarkEmitAsync(b *testing.B) {
	r, err := NewRabbus(Config{
		Dsn:      RABBUS_DSN,
		Attempts: 1,
		Timeout:  time.Second * 2,
		Durable:  false,
	})
	if err != nil {
		b.Errorf("Expected to init rabbus %s", err)
	}
	var wg sync.WaitGroup
	wg.Add(b.N)
	go func() {
		for {
			select {
			case <-r.EmitOk():
				wg.Done()
			case err := <-r.EmitErr():
				b.Fatalf("Expected to emit message, receive error: %v", err)
			}
		}
	}()

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
