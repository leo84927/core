package rabbitmq

import (
	"context"
	"log"
	"runtime/debug"
	"sync"

	"github.com/cenkalti/backoff/v5"
	"github.com/leo84927/core/logger"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rotisserie/eris"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

/**
 * prefetchCount 是這個系統唯一的併發度旋鈕：consumer 為每一則訊息開一個 goroutine，
 * broker 最多送出 prefetchCount 則未確認訊息，in-flight goroutine 數量因此被 broker 夾住。
 * 不做 worker pool —— 那會變成第二個旋鈕，且與 prefetch 互相打架。
 * 寫死在 core，三個 consumer 服務同值；要調整就是改這一行。
 */
const prefetchCount = 4

type Consumer struct {
	cm    *ConnectionManager
	queue string
	tag   string
}

type Message struct {
	Body []byte
}

type PublishHandler func(ctx context.Context, exchange, key string, body []byte) error

/*
 * MsgHandler 只回傳 error：worker 一律同步執行，回傳代表工作真的做完了。
 * 沒有 requeue 旗標 —— 否認一律 Nack(false, false)，詳見 docs/adr/0002。
 */
type MsgHandler func(context.Context, Message, PublishHandler) error

func (cm *ConnectionManager) NewConsumer(queue, tag string) *Consumer {
	// 不在這裡建立 channel，延遲到 consume 時才建
	return &Consumer{
		cm:    cm,
		queue: queue,
		tag:   tag,
	}
}

func (c *Consumer) WaitForConsume(ctx context.Context, handler MsgHandler) error {
	operation := func() (struct{}, error) {
		err := c.subscribeAndWait(ctx, handler)
		return struct{}{}, permanentIfNeeded(err)
	}

	budget := c.cm.Config.consume
	_, err := backoff.Retry(
		ctx,
		operation,
		backoff.WithMaxTries(budget.maxRetries),
		backoff.WithMaxElapsedTime(budget.maxElapsedTime),
	)
	return unwrapPermanent(err)
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
		return eris.Wrap(err, "failed to open a channel")
	}
	defer func() {
		/**
		 * 1. 滿足 linter errcheck
		 * 2. 閉包捕捉的是 reference，所以假如 ch 會因為 retry 而重新建立，也會關閉最新的值
		 */
		_ = ch.Close()
	}()

	/**
	 * WaitGroup：等 in-flight 的 per-message goroutine 全部做完才離開。
	 * 區域變數而非 Consumer 欄位 —— 每次 subscribeAndWait 都是一組全新的 goroutine，
	 * 重連後的新一輪不該等到上一輪的殘骸。
	 *
	 * 這個 defer 寫在 ch.Close() 之後，所以 LIFO 下它先跑：所有 in-flight goroutine 完成才關 channel。
	 * 反過來的話 in-flight 的 Ack 會打在已關閉的 channel 上，訊息一律被 broker 重送，
	 * 等於白等一場。
	 * 不設 timeout —— 實際上限由 systemd 的 TimeoutStopSec 提供。
	 */
	var wg sync.WaitGroup
	defer wg.Wait()

	// 限制未確認消息數量，避免消費者一次拿太多消息導致記憶體不足
	err = ch.Qos(
		prefetchCount, // 預取數量。定義消費者在未發送 Ack 之前，最多能持有的「未確認消息」數量。
		0,             // 預取大小（單位：Byte）。定義伺服器可發送的未確認消息總內容大小。0 代表不限制。
		false,         // true：對與該 channel 相同 connection 的所有 channel 生效。false：僅對當前 channel 生效。
	)
	if err != nil {
		log.Println("failed to set QoS:", err.Error())
		return eris.Wrap(err, "failed to set QoS")
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
		return eris.Wrap(err, "failed to register a consumer")
	}

	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				// 當連線異常時，ok 會是 false，此時停止 consumer 並讓外層重試
				log.Println("channel closed, exiting consumer")
				return eris.New("channel closed")
			}

			/**
			 * 併發歸 core：每一則訊息一個 goroutine，服務端的 worker 一律同步執行。
			 * for/select 因此不會被單一則訊息擋住，未確認訊息數才真的會增加，prefetch 才有意義。
			 */
			wg.Add(1)
			go func(d amqp.Delivery) {
				defer wg.Done()
				c.handleDelivery(ctx, &amqpDelivery{&d}, Message{Body: d.Body}, d.Headers, handler)
			}(d)

		case <-ctx.Done():
			log.Println("context cancelled, shutting down consumer")
			return nil
		}
	}
}

func (c *Consumer) handleDelivery(ctx context.Context, d AMQPDelivery, msg Message, headers amqp.Table, handler MsgHandler) {
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

	if err := c.runHandler(ctx, msg, handler); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		logger.Error(ctx, "failed to handle message", err)

		/**
		 * ctx 若取消，所有 ctx-aware 的對外呼叫會立刻回 context.Canceled，那是「還沒做完」而不是「做了但失敗」。
		 * 此時如果執行 Nack(false, false)，變成是「做了但失敗」而不是「還沒做完」，在沒有 DLX 的情況下等於把訊息刪掉，違反 at-least-once。
		 *
		 * 所以這裡先判斷 ctx.Err()，若取消就直接 return，訊息留著不確認，broker 會在 channel 關閉時把未確認的訊息重新入隊。
		 */
		if ctx.Err() != nil {
			return
		}

		/**
		 * Nack 代表訊息處理失敗
		 * multiple：是否批次確認，true 代表確認該訊息以及之前的訊息，false 代表只確認該訊息
		 *   per-message goroutine 併發確認，只能是 false
		 * requeue：恆為 false。core 不做錯誤分類，分不出暫時性與永久性失敗，requeue=true 對永久性失敗就是無限迴圈。
		 *   保留這個參數作為日後接上 dead-letter exchange 的零成本選項，詳見 docs/adr/0002
		 */
		if err := d.Nack(false, false); err != nil {
			logger.Error(ctx, "failed to nack message", err)
		}

		return
	}

	/**
	 * Ack 代表訊息處理成功
	 * multiple：同上
	 */
	if err := d.Ack(false); err != nil {
		logger.Error(ctx, "failed to ack message", err)
		return
	}

	log.Println("message processed successfully")
}

/*
 * runHandler 把 worker 的 panic 收斂成一則訊息的失敗：per-message goroutine 若沒有捕捉 panic，
 * 整個 process 會被帶走，其餘 in-flight 的訊息一起消失。
 *
 * panic 的現場只存在於 debug.Stack()，eris 是在 recover 這一刻才擷取堆疊，抓不到 panic 那一行，
 * 所以現場堆疊直接寫進錯誤訊息裡。
 */
func (c *Consumer) runHandler(ctx context.Context, msg Message, handler MsgHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = eris.Errorf("recovered from panic in message handler: %v\n%s", r, debug.Stack())
		}
	}()

	return handler(ctx, msg, c.cm.PublishWithRetry)
}
