package middleware

import (
	"context"
)

type QueueMiddleware struct {
	baseMiddleware
	queue string
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	msgs, err := qm.consumerChannel.Consume(
		qm.queue,       // queue
		qm.consumerTag, // consumer
		false,          // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)

	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	return qm.runConsumerLoop(msgs, callbackFunc)
}

func (qm *QueueMiddleware) StopConsuming() error {
	return qm.baseMiddleware.stopConsuming()
}

func (qm *QueueMiddleware) Send(msg Message) error {
	// se opta por usar un ctx para mantenernos en un tipo limite
	// y seguir la propuesta de RabbitMQ
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()
	return qm.publish(ctx, defaultExchange, qm.queue, msg)
}

func (qm *QueueMiddleware) Close() error {
	return qm.baseMiddleware.close()
}
