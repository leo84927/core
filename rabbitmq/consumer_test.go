package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ─────────────────────────────────────────────
// mockDelivery：實作 AMQPDelivery interface
// ─────────────────────────────────────────────

type mockDelivery struct {
	ackFunc      func(multiple bool) error
	nackFunc     func(multiple bool, requeue bool) error
	ackCalled    bool
	nackCalled   bool
	requeuedWith bool
}

func (m *mockChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	if m.qosFunc != nil {
		return m.qosFunc(prefetchCount, prefetchSize, global)
	}
	return nil
}

func (m *mockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	if m.consumeFunc != nil {
		return m.consumeFunc(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	}
	return nil, nil
}

func (m *mockDelivery) Ack(multiple bool) error {
	m.ackCalled = true
	if m.ackFunc != nil {
		return m.ackFunc(multiple)
	}
	return nil
}

func (m *mockDelivery) Nack(multiple bool, requeue bool) error {
	m.nackCalled = true
	m.requeuedWith = requeue
	if m.nackFunc != nil {
		return m.nackFunc(multiple, requeue)
	}
	return nil
}

// ─────────────────────────────────────────────
// Helper
// ─────────────────────────────────────────────

func newTestConsumer(cm *ConnectionManager) *Consumer {
	return cm.NewConsumer("test.queue", "test-tag", 1, 1*time.Second)
}

// 建立一個送出指定訊息後關閉的 msgs channel
func deliveryChannel(msgs ...amqp.Delivery) <-chan amqp.Delivery {
	ch := make(chan amqp.Delivery, len(msgs))
	for _, msg := range msgs {
		ch <- msg
	}
	close(ch)
	return ch
}

// ─────────────────────────────────────────────
// Tests: subscribeAndWait 流程
// ─────────────────────────────────────────────

// connect() 失敗時，應回傳 error
func TestWaitForConsume_ConnectFails(t *testing.T) {
	cm := newTestConnectionManager() // conn 為 nil，沒有 broker

	consumer := newTestConsumer(cm)
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) (bool, error) {
		return false, nil
	})

	if err == nil {
		t.Fatal("expected error when connect fails")
	}
}

// Channel() 失敗時，應回傳 error
func TestWaitForConsume_ChannelFails(t *testing.T) {
	mock := newMockConn()
	mock.channelFunc = func() (AMQPChannel, error) {
		return nil, errors.New("channel open failed")
	}

	cm := newTestConnectionManager()
	cm.conn = mock

	consumer := newTestConsumer(cm)
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) (bool, error) {
		return false, nil
	})

	if err == nil {
		t.Fatal("expected error when channel fails")
	}
}

// Qos() 失敗時，應回傳 error
func TestWaitForConsume_QosFails(t *testing.T) {
	ch := &mockChannel{
		qosFunc: func(prefetchCount, prefetchSize int, global bool) error {
			return errors.New("qos failed")
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	consumer := newTestConsumer(cm)
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) (bool, error) {
		return false, nil
	})

	if err == nil {
		t.Fatal("expected error when qos fails")
	}
}

// Consume() 失敗時，應回傳 error
func TestWaitForConsume_ConsumeFails(t *testing.T) {
	ch := &mockChannel{
		consumeFunc: func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			return nil, errors.New("consume failed")
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	consumer := newTestConsumer(cm)
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) (bool, error) {
		return false, nil
	})

	if err == nil {
		t.Fatal("expected error when consume fails")
	}
}

// msgs channel 關閉時（連線異常），應回傳 error 讓外層重試
// MaxRetries=1，最終結束並回傳 error
func TestWaitForConsume_MsgsChannelClosed(t *testing.T) {
	ch := &mockChannel{
		consumeFunc: func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			return deliveryChannel(), nil // 空的且已關閉
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	consumer := newTestConsumer(cm)
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) (bool, error) {
		return false, nil
	})

	if err == nil {
		t.Fatal("expected error when msgs channel is closed")
	}
}

// ctx 取消時應正常結束，回傳 nil
func TestWaitForConsume_ContextCancel(t *testing.T) {
	ready := make(chan struct{})

	ch := &mockChannel{
		consumeFunc: func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			close(ready)
			return make(chan amqp.Delivery), nil // 永遠不關閉，讓 consumer 等待
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	consumer := newTestConsumer(cm)
	go func() {
		done <- consumer.WaitForConsume(ctx, func(ctx context.Context, msg Message, _ PublishHandler) (bool, error) {
			return false, nil
		})
	}()

	// 確保進入 for/select 後再 cancel
	<-ready
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil on context cancel, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForConsume did not return after context cancel")
	}
}

// 成功後 channel 應被 Close()
func TestWaitForConsume_ChannelClosedAfterDone(t *testing.T) {
	ch := &mockChannel{
		consumeFunc: func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			return deliveryChannel(), nil
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	consumer := newTestConsumer(cm)
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) (bool, error) {
		return false, nil
	})

	if !ch.closed {
		t.Fatal("expected channel to be closed after WaitForConsume")
	}
	if err != nil && err.Error() != "channel closed" {
		t.Fatalf("expected channel closed, got: %v", err)
	}
}

// ─────────────────────────────────────────────
// Tests: handleDelivery
// ─────────────────────────────────────────────

// handler 成功時，應呼叫 Ack
func TestHandleDelivery_HandlerSuccess_CallsAck(t *testing.T) {
	d := &mockDelivery{}
	cm := newTestConnectionManager()
	consumer := newTestConsumer(cm)

	consumer.handleDelivery(context.Background(), d, Message{Body: []byte("hello")}, func(ctx context.Context, msg Message, _ PublishHandler) (bool, error) {
		return false, nil
	})

	if !d.ackCalled {
		t.Fatal("expected Ack to be called on success")
	}
	if d.nackCalled {
		t.Fatal("expected Nack not to be called on success")
	}
}

// handler 失敗且 requeue=true 時，應呼叫 Nack(requeue=true)
func TestHandleDelivery_HandlerFails_Requeue(t *testing.T) {
	d := &mockDelivery{}
	cm := newTestConnectionManager()
	consumer := newTestConsumer(cm)

	consumer.handleDelivery(context.Background(), d, Message{Body: []byte("hello")}, func(ctx context.Context, msg Message, _ PublishHandler) (bool, error) {
		return true, errors.New("handler failed")
	})

	if d.ackCalled {
		t.Fatal("expected Ack not to be called on failure")
	}
	if !d.nackCalled {
		t.Fatal("expected Nack to be called on failure")
	}
	if !d.requeuedWith {
		t.Fatal("expected Nack to be called with requeue=true")
	}
}

// handler 失敗且 requeue=false 時，應呼叫 Nack(requeue=false)
func TestHandleDelivery_HandlerFails_NoRequeue(t *testing.T) {
	d := &mockDelivery{}
	cm := newTestConnectionManager()
	consumer := newTestConsumer(cm)

	consumer.handleDelivery(context.Background(), d, Message{Body: []byte("hello")}, func(ctx context.Context, msg Message, _ PublishHandler) (bool, error) {
		return false, errors.New("handler failed")
	})

	if !d.nackCalled {
		t.Fatal("expected Nack to be called on failure")
	}
	if d.requeuedWith {
		t.Fatal("expected Nack to be called with requeue=false")
	}
}

// handler 收到的 Message.Body 應和 delivery 的一致
func TestHandleDelivery_PassesCorrectBody(t *testing.T) {
	d := &mockDelivery{}
	cm := newTestConnectionManager()
	consumer := newTestConsumer(cm)

	var receivedBody []byte
	consumer.handleDelivery(context.Background(), d, Message{Body: []byte("test-body")}, func(ctx context.Context, msg Message, _ PublishHandler) (bool, error) {
		receivedBody = msg.Body
		return false, nil
	})

	if string(receivedBody) != "test-body" {
		t.Errorf("expected body %q, got %q", "test-body", string(receivedBody))
	}
}
