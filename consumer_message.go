package rabbus

import (
	"time"

	"github.com/streadway/amqp"
)

type (
	// ConsumerMessage captures the fields for a previously delivered message resident in a queue
	// to be delivered by the server to a consumer.
	ConsumerMessage struct {
		delivery        amqp.Delivery
		ContentType     string
		ContentEncoding string
		// DeliveryMode queue implementation use, non-persistent (1) or persistent (2)
		DeliveryMode uint8
		// Priority queue implementation use, 0 to 9
		Priority uint8
		// CorrelationId application use, correlation identifier
		CorrelationId string
		// ReplyTo application use, address to to reply to (ex: RPC)
		ReplyTo string
		// Expiration implementation use, message expiration spec
		Expiration string
		// MessageId application use, message identifier
		MessageId string
		// Timestamp application use, message timestamp
		Timestamp time.Time
		// Type application use, message type name
		Type string
		// ConsumerTag valid only with Channel.Consume
		ConsumerTag string
		// MessageCount valid only with Channel.Get
		MessageCount uint32
		DeliveryTag  uint64
		Redelivered  bool
		Exchange     string
		// Headers application or header exchange table
		Headers map[string]interface{}
		// Key basic.publish routing key
		Key  string
		Body []byte
	}
)

func newConsumerMessage(m amqp.Delivery) ConsumerMessage {
	return ConsumerMessage{
		delivery:        m,
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		DeliveryMode:    m.DeliveryMode,
		Priority:        m.Priority,
		CorrelationId:   m.CorrelationId,
		ReplyTo:         m.ReplyTo,
		Expiration:      m.Expiration,
		Timestamp:       m.Timestamp,
		Type:            m.Type,
		ConsumerTag:     m.ConsumerTag,
		MessageCount:    m.MessageCount,
		DeliveryTag:     m.DeliveryTag,
		Redelivered:     m.Redelivered,
		Exchange:        m.Exchange,
		Headers:         m.Headers,
		Key:             m.RoutingKey,
		Body:            m.Body,
	}
}

// Ack delegates an acknowledgement through the Acknowledger interface that the client or server has finished work on a delivery.
// All deliveries in AMQP must be acknowledged. If you called Channel.Consume with autoAck true then the server will be automatically ack each message and this method should not be called. Otherwise, you must call Delivery.Ack after you have successfully processed this delivery.
// When multiple is true, this delivery and all prior unacknowledged deliveries on the same channel will be acknowledged. This is useful for batch processing of deliveries.
// An error will indicate that the acknowledge could not be delivered to the channel it was sent from.
// Either Delivery.Ack, Delivery.Reject or Delivery.Nack must be called for every delivery that is not automatically acknowledged.
func (cm ConsumerMessage) Ack(multiple bool) error {
	return cm.delivery.Ack(multiple)
}

// Nack negatively acknowledge the delivery of message(s) identified by the delivery tag from either the client or server.
// When multiple is true, nack messages up to and including delivered messages up until the delivery tag delivered on the same channel.
// When requeue is true, request the server to deliver this message to a different consumer. If it is not possible or requeue is false, the message will be dropped or delivered to a server configured dead-letter queue.
// This method must not be used to select or requeue messages the client wishes not to handle, rather it is to inform the server that the client is incapable of handling this message at this time.
// Either Delivery.Ack, Delivery.Reject or Delivery.Nack must be called for every delivery that is not automatically acknowledged.
func (cm ConsumerMessage) Nack(multiple, requeue bool) error {
	return cm.delivery.Nack(multiple, requeue)
}

// Reject delegates a negatively acknowledgement through the Acknowledger interface.
// When requeue is true, queue this message to be delivered to a consumer on a different channel. When requeue is false or the server is unable to queue this message, it will be dropped.
// If you are batch processing deliveries, and your server supports it, prefer Delivery.Nack.
// Either Delivery.Ack, Delivery.Reject or Delivery.Nack must be called for every delivery that is not automatically acknowledged.
func (cm ConsumerMessage) Reject(requeue bool) error {
	return cm.delivery.Reject(requeue)
}
