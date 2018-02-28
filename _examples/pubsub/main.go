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
		Exchange: "pubsub_test_ex",
		Kind:     "direct",
		Key:      "pubsub_test_key",
		Queue:    "pubsub_test_q",
	})
	if err != nil {
		log.Fatalf("Failed to create listener %s", err)
		return
	}

	wg.Add(1)
	go func(messages chan rabbus.ConsumerMessage) {
		for m := range messages {
			m.Ack(false)
			close(messages)
			wg.Done()
			log.Println("Message was consumed")
		}
	}(messages)

	msg := rabbus.Message{
		Exchange:     "pubsub_test_ex",
		Kind:         "direct",
		Key:          "pubsub_test_key",
		Payload:      []byte(`foo`),
		DeliveryMode: rabbus.Persistent,
	}

	r.EmitAsync() <- msg

outer:
	for {
		select {
		case <-r.EmitOk():
			log.Println("Message was sent")
			wg.Wait()
			log.Println("Done!")
			break outer
		case err := <-r.EmitErr():
			log.Fatalf("Failed to send message %s", err)
			break outer
		case <-timeout:
			log.Fatal("Timeout error during send message")
			break outer
		}
	}
}
