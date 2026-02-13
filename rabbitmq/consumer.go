package rabbitmq

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	ch   *amqp.Channel
	msgs <-chan amqp.Delivery
	tag  string
}

type Message struct {
	Body []byte
}

type MsgHandler func(Message) error

func NewConsumer(queue, tag string) *Consumer {
	// 宣告 channel
	ch, err := GetConn().Channel()
	if err != nil {
		log.Println("failed to open a channel:", err.Error())
		panic(err)
	}

	// 訂閱 queue
	msgs, err := ch.Consume(
		queue,
		tag,   // consumer tag（用來識別 consumer，同一 channel 內不可重複，但不同 channel 可以重複）
		false, // autoAck（true 代表自動 ack，當 rabbitmq 收到 ack 代表訊息已處理完畢，訊息會被刪除）
		false, // exclusive（是否排他，若為 true 則該 queue 只能被一個 consumer 消費）
		false, // noLocal（是否禁止將訊息發回給同一連線的 producer）
		false, // noWait（是否非同步，若為 true 則不等待 rabbitmq 回應，當 rabbitmq 異常時無法立即發現）
		nil,
	)
	if err != nil {
		log.Println("failed to register a consumer:", err.Error())
		ch.Close()
		panic(err)
	}

	return &Consumer{
		ch:   ch,
		msgs: msgs,
		tag:  tag,
	}
}

func (c *Consumer) WaitForConsume(ctx context.Context, handler MsgHandler) {
	defer c.cleanup()

	for {
		select {
		case d, ok := <-c.msgs:
			// 當連線異常時，ok 會是 false，此時關閉 consumer
			if !ok {
				log.Println("channel closed, exiting consumer")
				return
			}

			c.handleDelivery(d, handler)

		case <-ctx.Done():
			log.Println("context cancelled, shutting down consumer")
			return
		}
	}
}

func (c *Consumer) cleanup() {
	c.ch.Cancel(c.tag, false)
	c.ch.Close()
}

func (c *Consumer) handleDelivery(d amqp.Delivery, handler MsgHandler) {
	msg := Message{
		Body: d.Body,
	}

	/**
	 * Nack 代表訊息處理失敗
	 * multiple：是否批次確認，true 代表確認該訊息以及之前的訊息，false 代表只確認該訊息
	 * 通常建議序列處理時可以設定為 true，但併發處理時設定為 false
	 * requeue：是否重新入隊，true 代表重新入隊，false 代表丟棄
	 * 若為可重試的錯誤（例如網路異常），建議 requeue，若為不可重試的錯誤則建議丟棄
	 */
	if err := handler(msg); err != nil {
		log.Println("failed to handle message:", err.Error())

		if err := d.Nack(false, true); err != nil {
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
