package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type AMQPConnection interface {
	IsClosed() bool
	Close() error
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	Channel() (AMQPChannel, error)
}

type AMQPChannel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Confirm(noWait bool) error
	PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error)
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Cancel(consumer string, noWait bool) error
	Close() error
}

type amqpConnection struct {
	*amqp.Connection
}

type amqpChannel struct {
	*amqp.Channel
}

func (c *amqpConnection) IsClosed() bool {
	return c.Connection.IsClosed()
}

func (c *amqpConnection) Close() error {
	return c.Connection.Close()
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

func (c *amqpChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return c.Channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (c *amqpChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return c.Channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (c *amqpChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return c.Channel.QueueBind(name, key, exchange, noWait, args)
}

func (c *amqpChannel) Confirm(noWait bool) error {
	return c.Channel.Confirm(noWait)
}

func (c *amqpChannel) PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	return c.Channel.PublishWithDeferredConfirm(exchange, key, mandatory, immediate, msg)
}

func (c *amqpChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return c.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (c *amqpChannel) Cancel(consumer string, noWait bool) error {
	return c.Channel.Cancel(consumer, noWait)
}

func (c *amqpChannel) Close() error {
	return c.Channel.Close()
}
