package rabbus

import (
	"time"

	"github.com/streadway/amqp"
)

type ConsumerMessage struct {
	delivery        amqp.Delivery
	ContentType     string
	ContentEncoding string
	DeliveryMode    uint8     // queue implementation use - non-persistent (1) or persistent (2)
	Priority        uint8     // queue implementation use - 0 to 9
	CorrelationId   string    // application use - correlation identifier
	ReplyTo         string    // application use - address to to reply to (ex: RPC)
	Expiration      string    // implementation use - message expiration spec
	MessageId       string    // application use - message identifier
	Timestamp       time.Time // application use - message timestamp
	Type            string    // application use - message type name
	ConsumerTag     string    // Valid only with Channel.Consume
	MessageCount    uint32    // Valid only with Channel.Get
	DeliveryTag     uint64
	Redelivered     bool
	Exchange        string
	Key             string // basic.publish routing key
	Body            []byte
}

func (cm *ConsumerMessage) Ack(multiple bool) error {
	return cm.delivery.Ack(multiple)
}

func (cm *ConsumerMessage) Nack(multiple, requeue bool) error {
	return cm.delivery.Nack(multiple, requeue)
}

func (cm *ConsumerMessage) Reject(requeue bool) error {
	return cm.delivery.Reject(requeue)
}
