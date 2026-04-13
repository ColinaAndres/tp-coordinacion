package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	consumerChannel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	publisherChannel, err := conn.Channel()
	if err != nil {
		consumerChannel.Close()
		conn.Close()
		return nil, err
	}

	q, err := consumerChannel.QueueDeclare(
		queueName, // name
		false,     // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,
	)

	if err != nil {
		publisherChannel.Close()
		consumerChannel.Close()
		conn.Close()
		return nil, err
	}

	return &QueueMiddleware{
		baseMiddleware: baseMiddleware{
			conn:             conn,
			consumerChannel:  consumerChannel,
			publisherChannel: publisherChannel,
			consumerTag:      SimpleCryptoID(32),
		},
		queue: q.Name,
	}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	consumerChannel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	publisherChannel, err := conn.Channel()
	if err != nil {
		consumerChannel.Close()
		conn.Close()
		return nil, err
	}

	// Dado que me dan la lista de routing keys, asumo que el exchange es de tipo topic o direct
	err = publisherChannel.ExchangeDeclare(
		exchange, // name
		"topic",  // type
		false,    // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		publisherChannel.Close()
		consumerChannel.Close()
		conn.Close()
		return nil, err
	}

	return &ExchangeMiddleware{
		baseMiddleware: baseMiddleware{
			conn:             conn,
			consumerChannel:  consumerChannel,
			publisherChannel: publisherChannel,
			consumerTag:      SimpleCryptoID(32),
		},
		exchange: exchange,
		keys:     keys,
	}, nil
}
