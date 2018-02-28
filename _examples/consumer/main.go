package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/rafaeljesus/rabbus"
)

var (
	rabbusDsn = "amqp://localhost:5672"
	timeout   = time.After(time.Second * 3)
	wg        sync.WaitGroup
)

func main() {
	cbStateChangeFunc := func(name, from, to string) {
		// do something when state is changed
	}
	r, err := rabbus.New(
		rabbusDsn,
		rabbus.Durable(true),
		rabbus.Attempts(5),
		rabbus.Sleep(time.Second*2),
		rabbus.Threshold(3),
		rabbus.OnStateChange(cbStateChangeFunc),
	)
	if err != nil {
		log.Fatalf("Failed to init rabbus connection %s", err)
		return
	}

	defer func(r *rabbus.Rabbus) {
		if err := r.Close(); err != nil {
			log.Fatalf("Failed to close rabbus connection %s", err)
		}
	}(r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.Run(ctx)

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
