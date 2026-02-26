package rabbitmq

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rotisserie/eris"
)

func (cm *ConnectionManager) Publish(exchange, key string, body []byte) error {
	ch, err := cm.GetConn().Channel()
	if err != nil {
		return eris.Wrap(err, "failed to open a channel")
	}
	defer ch.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		exchange, // exchange
		key,      // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	if err != nil {
		return eris.Wrap(err, "failed to publish message")
	}

	return nil
}
