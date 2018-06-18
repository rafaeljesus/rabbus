package rabbus

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rafaeljesus/rabbus"
)

const (
	amqpDsnEnv        = "AMQP_DSN"
	amqpManagementEnv = "AMQP_MANAGEMENT_PORT"
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

func TestPublishDisconnect(t *testing.T) {
	if os.Getenv(amqpDsnEnv) == "" || os.Getenv(amqpManagementEnv) == "" {
		t.SkipNow()
	}

	amqpDsn = os.Getenv(amqpDsnEnv)
	amqpManagement := os.Getenv(amqpManagementEnv)

	r, err := rabbus.New(
		amqpDsn,
		rabbus.Durable(true),
		rabbus.Attempts(2),
		rabbus.BreakerTimeout(time.Millisecond*2),
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

	msg := rabbus.Message{
		Exchange:     "test_ex_kill",
		Kind:         rabbus.ExchangeDirect,
		Key:          "test_key",
		Payload:      []byte(`foo`),
		DeliveryMode: rabbus.Persistent,
	}

	r.EmitAsync() <- msg
	select {
	case <-r.EmitOk():

	case <-r.EmitErr():
		t.Error("expected to emit message")
	case <-timeout:
		t.Error("parallel.Run() failed, got timeout error")
	}
	killConnections(t, amqpManagement)

	r.EmitAsync() <- msg
	select {
	case <-r.EmitOk():

	case <-r.EmitErr():
		t.Error("expected to emit message")
	case <-time.After(5 * time.Second):
		t.Error("parallel.Run() failed, got timeout error")
	}

}

type amqpConnection struct {
	Name string `json:"name"`
}

// return all connections opened in rabbitmq via the management API
func getConnections(amqpManagement string) ([]amqpConnection, error) {
	client := &http.Client{}
	url := fmt.Sprintf("%s/connections", amqpManagement)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth("guest", "guest")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Non 200 response: %s", resp.Status)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	r := make([]amqpConnection, 0)
	err = json.Unmarshal(b, &r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// close one connection
func killConnection(connection amqpConnection, amqpManagement string) error {
	client := &http.Client{}
	url := fmt.Sprintf("%s/connections/%s", amqpManagement, connection.Name)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth("guest", "guest")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("deletion of connection failed, status: %s", resp.Status)
	}
	return nil
}

// close all open connections to the rabbitmq via the management api
func killConnections(t *testing.T, amqpManagement string) {
	conns := make(chan []amqpConnection)
	go func() {
		for {
			connections, err := getConnections(amqpManagement)
			if err != nil {
				t.Error(err)
			}
			if len(connections) >= 1 {
				conns <- connections
				break //exit the loop
			}
			//the rabbitmq api is a bit slow to update, we have to wait a bit
			time.Sleep(time.Second)
		}
	}()
	select {
	case connections := <-conns:
		for _, c := range connections {
			t.Log(c.Name)
			if err := killConnection(c, amqpManagement); err != nil {
				t.Errorf("impossible to kill connection (%s): %s", c.Name, err)
			}
		}
	case <-time.After(time.Second * 10):
		t.Error("timeout for killing connection reached")
	}
}
