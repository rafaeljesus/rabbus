package main

import (
	"context"
	"log"
	"time"

	"github.com/rafaeljesus/rabbus"
)

var (
	rabbusDsn = "amqp://localhost:5672"
	timeout   = time.After(time.Second * 3)
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
			break outer
		case err := <-r.EmitErr():
			log.Fatalf("Failed to send message %s", err)
			break outer
		case <-timeout:
			log.Println("got time out error")
			break outer
		}
	}
}
