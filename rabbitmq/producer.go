package rabbitmq

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (cm *ConnectionManager) PublishWithRetry(exchange, key string, body []byte, maxRetries uint, maxElapsedTime time.Duration) error {
	operation := func() (struct{}, error) {
		conn, err := cm.connect()
		if err != nil {
			return struct{}{}, permanentIfNeeded(err)
		}
		ch, err := conn.Channel()
		if err != nil {
			return struct{}{}, permanentIfNeeded(err)
		}
		defer func() {
			_ = ch.Close()
		}()

		if err := ch.Confirm(false); err != nil {
			return struct{}{}, permanentIfNeeded(err)
		}

		confirm, err := ch.PublishWithDeferredConfirm(
			exchange, // exchange
			key,      // routing key
			false,    // mandatory
			false,    // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        body,
			},
		)
		if err != nil {
			return struct{}{}, permanentIfNeeded(err)
		}

		if confirmed := confirm.Wait(); !confirmed {
			return struct{}{}, errors.New("publish message failed")
		}

		return struct{}{}, nil
	}

	_, err := backoff.Retry(
		context.Background(),
		operation,
		backoff.WithMaxTries(maxRetries),
		backoff.WithMaxElapsedTime(maxElapsedTime),
	)
	return err
}
