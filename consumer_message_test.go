package rabbus

import (
	"testing"

	"github.com/streadway/amqp"
)

func TestConsumerMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			"ack message",
			testAckMessage,
		},
		{
			"nack message",
			testNackMessage,
		},
		{
			"reject message",
			testRejectMessage,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t)
		})
	}
}

func testAckMessage(t *testing.T) {
	ack := &acknowledger{}
	d := amqp.Delivery{Acknowledger: ack}
	cm := newConsumerMessage(d)
	cm.Ack(false)

	if !ack.ackInvoked {
		t.Fatal("expected acknowledger.Ack to be invoked")
	}
}

func testNackMessage(t *testing.T) {
	ack := &acknowledger{}
	d := amqp.Delivery{Acknowledger: ack}
	cm := newConsumerMessage(d)
	cm.Nack(false, false)

	if !ack.nackInvoked {
		t.Fatal("expected acknowledger.Nack to be invoked")
	}
}

func testRejectMessage(t *testing.T) {
	ack := &acknowledger{}
	d := amqp.Delivery{Acknowledger: ack}
	cm := newConsumerMessage(d)
	cm.Reject(false)

	if !ack.rejectInvoked {
		t.Fatal("expected acknowledger.Reject to be invoked")
	}
}

type acknowledger struct {
	ackInvoked, nackInvoked, rejectInvoked bool
}

func (a *acknowledger) Ack(tag uint64, multiple bool) error {
	a.ackInvoked = true
	return nil
}

func (a *acknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	a.nackInvoked = true
	return nil
}

func (a *acknowledger) Reject(tag uint64, requeue bool) error {
	a.rejectInvoked = true
	return nil
}
