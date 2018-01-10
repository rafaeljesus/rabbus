package main

import (
	"log"
	"sync"
	"time"

	"github.com/rafaeljesus/rabbus"
)

var (
	RABBUS_DSN = "amqp://localhost:5672"
	timeout    = time.After(time.Second * 3)
	wg         sync.WaitGroup
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

	messages, err := r.Listen(rabbus.ListenConfig{
		Exchange: "consumer_test_ex",
		Kind:     "direct",
		Key:      "consumer_test_key",
		Queue:    "consumer_test_q",
	})
	if err != nil {
		log.Fatalf("Failed to create listener %s", err)
		return
	}
	defer close(messages)

	for {
		log.Println("Listening for messages...")

		m, ok := <-messages
		if !ok {
			log.Println("Stop listening messages!")
			break
		}

		m.Ack(false)

		log.Println("Message was consumed")
	}
}
