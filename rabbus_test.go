package rabbus

import (
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

	if err := r.Listen(ListenConfig{
		Exchange: "test_ex",
		Kind:     "direct",
		Key:      "test_key",
		Queue:    "test_q",
		Handler: func(d *Delivery) {
			d.Ack(true)
			wg.Done()
		},
	}); err != nil {
		t.Errorf("Expected to listen message %s", err)
	}

	r.Emit() <- &Message{
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

	if err := r.Listen(ListenConfig{}); err == nil {
		t.Errorf("Expected to validate Exchange")
	}

	if err = r.Listen(ListenConfig{Exchange: "foo"}); err == nil {
		t.Errorf("Expected to validate Kind")
	}

	if err = r.Listen(ListenConfig{
		Exchange: "foo2",
		Kind:     "direct",
	}); err == nil {
		t.Errorf("Expected to validate Queue")
	}

	if err = r.Listen(ListenConfig{
		Exchange: "foo2",
		Kind:     "direct",
		Queue:    "foo2",
	}); err == nil {
		t.Errorf("Expected to validate Handler")
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

func TestRabbus_reconnect(t *testing.T) {
	r, err := NewRabbus(Config{
		Dsn:      RABBUS_DSN,
		Attempts: 1,
		Timeout:  time.Second * 2,
	})
	if err != nil {
		t.Errorf("Expected to init rabbus %s", err)
	}

	r.Close()
	r.reconnect()

	r.Emit() <- &Message{
		Exchange:     "foo2",
		Kind:         "direct",
		Key:          "foo2",
		Payload:      []byte(`foo`),
		DeliveryMode: Transient,
	}

loop:
	for {
		select {
		case <-r.EmitOk():
			break loop
		case <-r.EmitErr():
			t.Errorf("Expected to emit message")
			break loop
		}
	}
}
