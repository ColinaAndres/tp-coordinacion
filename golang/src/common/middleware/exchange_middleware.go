package middleware

import (
	"context"
)

type ExchangeMiddleware struct {
	baseMiddleware
	exchange string
	keys     []string
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	queue, err := em.consumerChannel.QueueDeclare(
		"",    // name
		false, // durability
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,
	)

	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	for _, key := range em.keys {
		err = em.consumerChannel.QueueBind(
			queue.Name,  // queue name
			key,         // routing key
			em.exchange, // exchange
			false,       // no-wait
			nil,
		)
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}

	msgs, err := em.consumerChannel.Consume(
		queue.Name,     // queue
		em.consumerTag, // consumer
		false,          // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	return em.runConsumerLoop(msgs, callbackFunc)
}

func (em *ExchangeMiddleware) StopConsuming() error {
	return em.baseMiddleware.stopConsuming()
}

func (em *ExchangeMiddleware) Send(msg Message) error {
	// se opta por usar un ctx para mantenernos en un tipo limite
	// y seguir la propuesta de RabbitMQ
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	for _, key := range em.keys {
		err := em.publish(ctx, em.exchange, key, msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	return em.baseMiddleware.close()
}
