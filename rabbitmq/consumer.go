package rabbitmq

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/cenkalti/backoff/v5"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Consumer struct {
	cm             *ConnectionManager
	queue          string
	tag            string
	MaxRetries     uint          // 最大重試次數上限
	MaxElpasedTime time.Duration // 總重試時間上限
}

type Message struct {
	Body []byte
}

type PublishHandler func(ctx context.Context, exchange, key string, body []byte, maxRetries uint, maxElapsedTime time.Duration) error

type MsgHandler func(context.Context, Message, PublishHandler) (requeue bool, err error)

func (cm *ConnectionManager) NewConsumer(queue, tag string, maxRetries uint, maxElpasedTime time.Duration) *Consumer {
	// 不在這裡建立 channel，延遲到 consume 時才建
	return &Consumer{
		cm:             cm,
		queue:          queue,
		tag:            tag,
		MaxRetries:     maxRetries,
		MaxElpasedTime: maxElpasedTime,
	}
}

func (c *Consumer) WaitForConsume(ctx context.Context, handler MsgHandler) error {
	operation := func() (struct{}, error) {
		err := c.subscribeAndWait(ctx, handler)
		return struct{}{}, permanentIfNeeded(err)
	}

	_, err := backoff.Retry(
		ctx,
		operation,
		backoff.WithMaxTries(c.MaxRetries),
		backoff.WithMaxElapsedTime(c.MaxElpasedTime),
	)
	return err
}

func (c *Consumer) subscribeAndWait(ctx context.Context, handler MsgHandler) error {
	conn, err := c.cm.connect(ctx)
	if err != nil {
		log.Println("failed to get connection, err:", err.Error())
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Println("failed to open a channel:", err.Error())
		return err
	}
	defer func() {
		/**
		 * 1. 滿足 linter errcheck
		 * 2. 閉包捕捉的是 reference，所以假如 ch 會因為 retry 而重新建立，也會關閉最新的值
		 */
		_ = ch.Close()
	}()

	// 限制未確認消息數量，避免消費者一次拿太多消息導致記憶體不足
	err = ch.Qos(
		1,     // 預取數量。定義消費者在未發送 Ack 之前，最多能持有的「未確認消息」數量。
		0,     // 預取大小（單位：Byte）。定義伺服器可發送的未確認消息總內容大小。0 代表不限制。
		false, // true：對與該 channel 相同 connection 的所有 channel 生效。false：僅對當前 channel 生效。
	)
	if err != nil {
		log.Println("failed to set QoS:", err.Error())
		return err
	}

	// 訂閱 queue
	msgs, err := ch.Consume(
		c.queue,
		c.tag, // consumer tag（用來識別 consumer，同一 channel 內不可重複，但不同 channel 可以重複）
		false, // autoAck（true 代表自動 ack，當 rabbitmq 收到 ack 代表訊息已處理完畢，訊息會被刪除）
		false, // exclusive（是否排他，若為 true 則該 queue 只能被一個 consumer 消費）
		false, // noLocal（是否禁止將訊息發回給同一連線的 producer）
		false, // noWait（是否非同步，若為 true 則不等待 rabbitmq 回應，當 rabbitmq 異常時無法立即發現）
		nil,
	)
	if err != nil {
		log.Println("failed to register a consumer:", err.Error())
		return err
	}

	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				// 當連線異常時，ok 會是 false，此時停止 consumer 並讓外層重試
				log.Println("channel closed, exiting consumer")
				return errors.New("channel closed")
			}

			c.handleDelivery(ctx, &amqpDelivery{&d}, Message{Body: d.Body}, d.Headers, handler)

		case <-ctx.Done():
			log.Println("context cancelled, shutting down consumer")
			return nil
		}
	}
}

func (c *Consumer) handleDelivery(ctx context.Context, d AMQPDelivery, msg Message, headers amqp.Table, handler MsgHandler) {
	/**
	 * Nack 代表訊息處理失敗
	 * multiple：是否批次確認，true 代表確認該訊息以及之前的訊息，false 代表只確認該訊息
	 * 通常建議序列處理時可以設定為 true，但併發處理時設定為 false
	 * requeue：是否重新入隊，true 代表重新入隊，false 代表丟棄
	 * 若為可重試的錯誤（例如網路異常），建議 requeue，若為不可重試的錯誤則建議丟棄
	 */
	ctx = otel.GetTextMapPropagator().Extract(ctx, amqpHeaderCarrier(headers))
	ctx, span := otel.Tracer("rabbitmq").Start(ctx, "consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", c.queue),
			attribute.String("messaging.operation.type", "receive"),
		),
	)
	defer span.End()

	if requeue, err := handler(ctx, msg, c.cm.PublishWithRetry); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Println("failed to handle message:", err.Error())

		if err := d.Nack(false, requeue); err != nil {
			log.Println("failed to nack message:", err.Error())
		}

		return
	}

	/**
	 * Ack 代表訊息處理成功
	 * multiple：同上
	 */
	if err := d.Ack(false); err != nil {
		log.Println("failed to ack message:", err.Error())
		return
	}

	log.Println("message processed successfully")
}
