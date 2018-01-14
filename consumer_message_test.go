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
			scenario: "ack message",
			function: testAckMessage,
		},
		{
			scenario: "nack message",
			function: testNackMessage,
		},
		{
			scenario: "reject message",
			function: testRejectMessage,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t)
		})
	}
}

func testAckMessage(t *testing.T) {
	ack := &acknowledgerMock{}
	d := amqp.Delivery{Acknowledger: ack}
	cm := newConsumerMessage(d)
	cm.Ack(false)

	if !ack.acked {
		t.Error("Expected to have called ack")
	}
}

func testNackMessage(t *testing.T) {
	ack := &acknowledgerMock{}
	d := amqp.Delivery{Acknowledger: ack}
	cm := newConsumerMessage(d)
	cm.Nack(false, false)

	if !ack.nacked {
		t.Error("Expected to have called nack")
	}
}

func testRejectMessage(t *testing.T) {
	ack := &acknowledgerMock{}
	d := amqp.Delivery{Acknowledger: ack}
	cm := newConsumerMessage(d)
	cm.Reject(false)

	if !ack.rejected {
		t.Error("Expected to have called reject")
	}
}

type acknowledgerMock struct {
	acked, nacked, rejected bool
}

func (a *acknowledgerMock) Ack(tag uint64, multiple bool) error {
	a.acked = true
	return nil
}

func (a *acknowledgerMock) Nack(tag uint64, multiple bool, requeue bool) error {
	a.nacked = true
	return nil
}

func (a *acknowledgerMock) Reject(tag uint64, requeue bool) error {
	a.rejected = true
	return nil
}
