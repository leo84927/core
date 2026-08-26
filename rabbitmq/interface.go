package rabbitmq

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AMQPConnection interface {
	IsClosed() bool
	CloseDeadline(deadline time.Time) error
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	Channel() (AMQPChannel, error)
}

type AMQPChannel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Confirm(noWait bool) error
	PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (AMQPDeferredConfirmation, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Close() error
}

type AMQPDeferredConfirmation interface {
	WaitContext(ctx context.Context) (bool, error)
}

type AMQPDelivery interface {
	Ack(multiple bool) error
	Nack(multiple bool, requeue bool) error
}

type amqpConnection struct {
	*amqp.Connection
}

type amqpChannel struct {
	*amqp.Channel
}

type amqpDeferredConfirmation struct {
	*amqp.DeferredConfirmation
}

type amqpDelivery struct {
	*amqp.Delivery
}

// ─────────────────────────────────────────────
// Connection
// ─────────────────────────────────────────────

func (c *amqpConnection) IsClosed() bool {
	return c.Connection.IsClosed()
}

/*
 * 關機路徑用 CloseDeadline()
 * 原本的 Close() 會無限期等 broker 回 close-ok，而它排在 OTLP flush 前面，卡住的話診斷資訊全滅
 */
func (c *amqpConnection) CloseDeadline(deadline time.Time) error {
	return c.Connection.CloseDeadline(deadline)
}

func (c *amqpConnection) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	return c.Connection.NotifyClose(ch)
}

func (c *amqpConnection) Channel() (AMQPChannel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}
	return &amqpChannel{ch}, nil
}

// ─────────────────────────────────────────────
// Topology
// ─────────────────────────────────────────────

func (c *amqpChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return c.Channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (c *amqpChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return c.Channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (c *amqpChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return c.Channel.QueueBind(name, key, exchange, noWait, args)
}

// ─────────────────────────────────────────────
// Producer
// ─────────────────────────────────────────────

func (c *amqpChannel) Confirm(noWait bool) error {
	return c.Channel.Confirm(noWait)
}

func (c *amqpChannel) PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (AMQPDeferredConfirmation, error) {
	confirm, err := c.Channel.PublishWithDeferredConfirm(exchange, key, mandatory, immediate, msg)
	if err != nil {
		return nil, err
	}
	return &amqpDeferredConfirmation{confirm}, nil
}

/*
 * 等待 broker 回 confirm 必須吃 ctx
 * 原本的 Wait() 沒有上限，而 consumer 的 handler 包含 publish，關機時若卡在這裡就只能等 systemd 強殺
 */
func (c *amqpDeferredConfirmation) WaitContext(ctx context.Context) (bool, error) {
	return c.DeferredConfirmation.WaitContext(ctx)
}

// ─────────────────────────────────────────────
// Consumer
// ─────────────────────────────────────────────

func (c *amqpChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return c.Channel.Qos(prefetchCount, prefetchSize, global)
}

func (c *amqpChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return c.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (d *amqpDelivery) Ack(multiple bool) error {
	return d.Delivery.Ack(multiple)
}

func (d *amqpDelivery) Nack(multiple bool, requeue bool) error {
	return d.Delivery.Nack(multiple, requeue)
}

// ─────────────────────────────────────────────
// Close
// ─────────────────────────────────────────────

func (c *amqpChannel) Close() error {
	return c.Channel.Close()
}
