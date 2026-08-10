package rabbitmq

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v5"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rotisserie/eris"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type amqpHeaderCarrier amqp.Table

func (c amqpHeaderCarrier) Get(key string) string {
	s, _ := c[key].(string)
	return s
}

func (c amqpHeaderCarrier) Set(key, value string) {
	c[key] = value
}

func (c amqpHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

func (cm *ConnectionManager) PublishWithRetry(ctx context.Context, exchange, key string, body []byte, maxRetries uint, maxElapsedTime time.Duration) error {
	ctx, span := otel.Tracer("rabbitmq").Start(ctx, "publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", exchange),
			attribute.String("messaging.rabbitmq.destination.routing_key", key),
			attribute.String("messaging.operation.type", "publish"),
		),
	)
	defer span.End()

	headers := amqp.Table{}
	otel.GetTextMapPropagator().Inject(ctx, amqpHeaderCarrier(headers))

	operation := func() (struct{}, error) {
		conn, err := cm.connect(ctx)
		if err != nil {
			return struct{}{}, permanentIfNeeded(err)
		}
		ch, err := conn.Channel()
		if err != nil {
			return struct{}{}, permanentIfNeeded(eris.Wrap(err, "failed to open a channel"))
		}
		defer func() {
			_ = ch.Close()
		}()

		if err := ch.Confirm(false); err != nil {
			return struct{}{}, permanentIfNeeded(eris.Wrap(err, "failed to put channel into confirm mode"))
		}

		confirm, err := ch.PublishWithDeferredConfirm(
			exchange,
			key,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Headers:     headers,
				Body:        body,
			},
		)
		if err != nil {
			return struct{}{}, permanentIfNeeded(eris.Wrap(err, "failed to publish message"))
		}

		if confirmed := confirm.Wait(); !confirmed {
			return struct{}{}, eris.New("publish message failed")
		}

		return struct{}{}, nil
	}

	_, err := backoff.Retry(
		ctx,
		operation,
		backoff.WithMaxTries(maxRetries),
		backoff.WithMaxElapsedTime(maxElapsedTime),
	)
	if err != nil {
		err = unwrapPermanent(err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}
