package middleware

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type baseMiddleware struct {
	conn             *amqp.Connection
	consumerChannel  *amqp.Channel
	publisherChannel *amqp.Channel
	consumerTag      string
	consumingWaiting sync.WaitGroup
}

func (b *baseMiddleware) runConsumerLoop(msgs <-chan amqp.Delivery, callbackFunc func(msg Message, ack func(), nack func())) error {
	b.consumingWaiting.Add(1)
	defer b.consumingWaiting.Done()

	var ackError error
	for msg := range msgs {
		callbackFunc(
			Message{Body: string(msg.Body)},
			func() {
				if err := msg.Ack(false); err != nil {
					ackError = ErrMessageMiddlewareMessage
				}
			},
			func() {
				if err := msg.Nack(false, true); err != nil {
					ackError = ErrMessageMiddlewareMessage
				}
			},
		)
		if ackError != nil {
			return ackError
		}
	}

	if b.consumerChannel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (b *baseMiddleware) stopConsuming() error {
	err := b.consumerChannel.Cancel(b.consumerTag, false)
	if err != nil && b.consumerChannel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	b.consumingWaiting.Wait()
	return nil
}

func (b *baseMiddleware) publish(ctx context.Context, exchange, routingKey string, msg Message) error {
	err := b.publisherChannel.PublishWithContext(ctx,
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: contentType,
			Body:        []byte(msg.Body),
		},
	)
	if b.publisherChannel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	return err
}

func (b *baseMiddleware) close() error {
	b.stopConsuming()
	if err := b.publisherChannel.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}
	if err := b.consumerChannel.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}
	if err := b.conn.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}
