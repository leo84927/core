package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ─────────────────────────────────────────────
// mockChannel：實作 AMQPChannel interface
// ─────────────────────────────────────────────

type mockChannel struct {
	closed                 bool
	exchangeDeclareFunc    func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	queueDeclareFunc       func(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	queueBindFunc          func(name, key, exchange string, noWait bool, args amqp.Table) error
	publishWithContextFunc func(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	consumeFunc            func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	cancelFunc             func(consumer string, noWait bool) error
}

func (m *mockChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	if m.exchangeDeclareFunc != nil {
		return m.exchangeDeclareFunc(name, kind, durable, autoDelete, internal, noWait, args)
	}
	return nil
}

func (m *mockChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if m.queueDeclareFunc != nil {
		return m.queueDeclareFunc(name, durable, autoDelete, exclusive, noWait, args)
	}
	return amqp.Queue{Name: name}, nil
}

func (m *mockChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	if m.queueBindFunc != nil {
		return m.queueBindFunc(name, key, exchange, noWait, args)
	}
	return nil
}

func (m *mockChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return nil
}
func (m *mockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return nil, nil
}
func (m *mockChannel) Cancel(consumer string, noWait bool) error {
	return nil
}

func (m *mockChannel) Close() error {
	m.closed = true
	return nil
}

// 編譯期確認 mockChannel 實作了 AMQPChannel
var _ AMQPChannel = (*mockChannel)(nil)

// ─────────────────────────────────────────────
// Helper
// ─────────────────────────────────────────────

func newTestTopology() Topology {
	return Topology{
		Exchange: Exchange{
			Name: "test.exchange",
			Kind: "direct",
		},
		Queues: []Queue{
			{
				Name: "test.queue.1",
				Keys: []string{"key.1", "key.2"},
			},
			{
				Name: "test.queue.2",
				Keys: []string{"key.3"},
			},
		},
		MaxElpasedTime: 1 * time.Second,
		MaxRetries:     1,
	}
}

func newMockConnWithChannel(ch AMQPChannel) *mockConn {
	mock := newMockConn()
	mock.channelFunc = func() (AMQPChannel, error) {
		return ch, nil
	}
	return mock
}

// ─────────────────────────────────────────────
// Tests: InitTopology()
// ─────────────────────────────────────────────

// 正常流程：所有 declare 都成功，topology 應被存入 cm.topology
func TestInitTopology_Success(t *testing.T) {
	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(&mockChannel{})

	err := cm.InitTopology(newTestTopology())
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if cm.topology.Load() == nil {
		t.Fatal("expected topology to be stored after successful init")
	}
}

// connect() 失敗時，InitTopology 應回傳 error 且不存入 topology
func TestInitTopology_ConnectFails(t *testing.T) {
	cm := newTestConnectionManager() // cm.conn 為 nil，沒有 broker

	err := cm.InitTopology(newTestTopology())
	if err == nil {
		t.Fatal("expected error when connect fails")
	}
	if cm.topology.Load() != nil {
		t.Fatal("expected topology not to be stored on failure")
	}
}

// Channel() 失敗時，InitTopology 應回傳 error 且不存入 topology
func TestInitTopology_ChannelFails(t *testing.T) {
	mock := newMockConn()
	mock.channelFunc = func() (AMQPChannel, error) {
		return nil, errors.New("channel open failed")
	}

	cm := newTestConnectionManager()
	cm.conn = mock

	err := cm.InitTopology(newTestTopology())
	if err == nil {
		t.Fatal("expected error when channel fails")
	}
	if cm.topology.Load() != nil {
		t.Fatal("expected topology not to be stored on failure")
	}
}

// ExchangeDeclare 失敗時，InitTopology 應回傳 error 且不存入 topology
func TestInitTopology_ExchangeDeclareFails(t *testing.T) {
	ch := &mockChannel{
		exchangeDeclareFunc: func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
			return errors.New("exchange declare failed")
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	err := cm.InitTopology(newTestTopology())
	if err == nil {
		t.Fatal("expected error when exchange declare fails")
	}
	if cm.topology.Load() != nil {
		t.Fatal("expected topology not to be stored on failure")
	}
}

// QueueDeclare 失敗時，InitTopology 應回傳 error
func TestInitTopology_QueueDeclareFails(t *testing.T) {
	ch := &mockChannel{
		queueDeclareFunc: func(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
			return amqp.Queue{}, errors.New("queue declare failed")
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	err := cm.InitTopology(newTestTopology())
	if err == nil {
		t.Fatal("expected error when queue declare fails")
	}
}

// QueueBind 失敗時，InitTopology 應回傳 error
func TestInitTopology_QueueBindFails(t *testing.T) {
	ch := &mockChannel{
		queueBindFunc: func(name, key, exchange string, noWait bool, args amqp.Table) error {
			return errors.New("queue bind failed")
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	err := cm.InitTopology(newTestTopology())
	if err == nil {
		t.Fatal("expected error when queue bind fails")
	}
}

// ─────────────────────────────────────────────
// Tests: declareTopology()
// ─────────────────────────────────────────────

// 驗證 ExchangeDeclare 被呼叫時，傳入的參數正確
func TestDeclareTopology_ExchangeParams(t *testing.T) {
	var capturedName, capturedKind string

	ch := &mockChannel{
		exchangeDeclareFunc: func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
			capturedName = name
			capturedKind = kind
			return nil
		},
	}

	cm := newTestConnectionManager()
	topology := newTestTopology()

	err := cm.declareTopology(ch, topology)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if capturedName != topology.Exchange.Name {
		t.Errorf("expected exchange name %q, got %q", topology.Exchange.Name, capturedName)
	}
	if capturedKind != topology.Exchange.Kind {
		t.Errorf("expected exchange kind %q, got %q", topology.Exchange.Kind, capturedKind)
	}
}

// 驗證 QueueDeclare 和 QueueBind 的呼叫次數符合 topology 定義
func TestDeclareTopology_QueueAndBindCallCount(t *testing.T) {
	queueDeclareCount := 0
	queueBindCount := 0

	ch := &mockChannel{
		queueDeclareFunc: func(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
			queueDeclareCount++
			return amqp.Queue{Name: name}, nil
		},
		queueBindFunc: func(name, key, exchange string, noWait bool, args amqp.Table) error {
			queueBindCount++
			return nil
		},
	}

	cm := newTestConnectionManager()
	topology := newTestTopology()

	err := cm.declareTopology(ch, topology)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expectedQueueDeclare := len(topology.Queues) // 2
	if queueDeclareCount != expectedQueueDeclare {
		t.Errorf("expected QueueDeclare called %d times, got %d", expectedQueueDeclare, queueDeclareCount)
	}

	// queue1: key.1, key.2 / queue2: key.3 → 共 3 次
	expectedQueueBind := 0
	for _, q := range topology.Queues {
		expectedQueueBind += len(q.Keys)
	}
	if queueBindCount != expectedQueueBind {
		t.Errorf("expected QueueBind called %d times, got %d", expectedQueueBind, queueBindCount)
	}
}

// 成功後 channel 應被 Close()
func TestDeclareTopology_ChannelClosedAfterSuccess(t *testing.T) {
	ch := &mockChannel{}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	cm.InitTopology(newTestTopology())
	if !ch.closed {
		t.Fatal("expected channel to be closed after InitTopology")
	}
}

// 失敗後 channel 也應被 Close()（defer 保證）
func TestDeclareTopology_ChannelClosedAfterFailure(t *testing.T) {
	ch := &mockChannel{
		exchangeDeclareFunc: func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
			return errors.New("exchange declare failed")
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	cm.InitTopology(newTestTopology())
	if !ch.closed {
		t.Fatal("expected channel to be closed even after failure")
	}
}
