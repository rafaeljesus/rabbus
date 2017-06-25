package main

import (
	"log"
	"sync"
	"time"

	"github.com/rafaeljesus/rabbus"
)

var (
	RABBUS_DSN = "amqp://localhost:5672"
	wg         sync.WaitGroup
)

func main() {
	r, err := rabbus.NewRabbus(rabbus.Config{
		Dsn:       RABBUS_DSN,
		Attempts:  1,
		Timeout:   time.Second * 2,
		Threshold: 3,
		Durable:   true,
	})
	if err != nil {
		log.Print(err)
	}

	messages, err := r.Listen(rabbus.ListenConfig{
		Exchange: "test_ex",
		Kind:     "direct",
		Key:      "test_key",
		Queue:    "test_q",
	})
	if err != nil {
		log.Print(err)
	}

	wg.Add(1)
	go func() {
		for m := range messages {
			m.Ack(false)
			wg.Done()
		}
	}()

	r.EmitAsync() <- rabbus.Message{
		Exchange:     "test_ex",
		Kind:         "direct",
		Key:          "test_key",
		Payload:      []byte(`foo`),
		DeliveryMode: rabbus.Persistent,
	}

	go func() {
		for {
			select {
			case <-r.EmitOk():
				log.Print("Message sent")
			case <-r.EmitErr():
				log.Print("Expected to emit message")
				wg.Done()
			}
		}
	}()

	wg.Wait()
}
