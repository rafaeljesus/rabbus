package main

import (
	"log"
	"time"

	"github.com/rafaeljesus/rabbus"
)

var (
	RABBUS_DSN = "amqp://localhost:5672"
	timeout    = time.After(time.Second * 3)
)

func main() {
	config := rabbus.Config{
		Dsn:     RABBUS_DSN,
		Durable: true,
		Retry: rabbus.Retry{
			Attempts: 5,
		},
		Breaker: rabbus.Breaker{
			Threshold: 3,
			OnStateChange: func(name, from, to string) {
				// do something when state is changed
			},
		},
	}

	r, err := rabbus.NewRabbus(config)
	if err != nil {
		log.Fatalf("Failed to init rabbus connection %s", err)
		return
	}

	defer func(r rabbus.Rabbus) {
		if err := r.Close(); err != nil {
			log.Fatalf("Failed to close rabbus connection %s", err)
		}
	}(r)

	msg := rabbus.Message{
		Exchange:     "producer_test_ex",
		Kind:         "direct",
		Key:          "producer_test_key",
		Payload:      []byte(`foo`),
		DeliveryMode: rabbus.Persistent,
	}

	r.EmitAsync() <- msg

outer:
	for {
		select {
		case <-r.EmitOk():
			log.Println("Message was sent")
		case err := <-r.EmitErr():
			log.Fatalf("Failed to send message %s", err)
			break outer
		case <-timeout:
			log.Println("Bye")
			break outer
		}
	}
}
