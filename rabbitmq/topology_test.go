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
	closed                         bool
	exchangeDeclareFunc            func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	queueDeclareFunc               func(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	queueBindFunc                  func(name, key, exchange string, noWait bool, args amqp.Table) error
	confirmFunc                    func(noWait bool) error
	publishWithDeferredConfirmFunc func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (AMQPDeferredConfirmation, error)
	consumeFunc                    func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	qosFunc                        func(prefetchCount, prefetchSize int, global bool) error
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

	err := cm.InitTopology(t.Context(), newTestTopology())
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

	err := cm.InitTopology(t.Context(), newTestTopology())
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

	err := cm.InitTopology(t.Context(), newTestTopology())
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

	err := cm.InitTopology(t.Context(), newTestTopology())
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

	err := cm.InitTopology(t.Context(), newTestTopology())
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

	err := cm.InitTopology(t.Context(), newTestTopology())
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

	err := cm.InitTopology(t.Context(), newTestTopology())
	if !ch.closed {
		t.Fatal("expected channel to be closed after InitTopology")
	}
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
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

	err := cm.InitTopology(t.Context(), newTestTopology())
	if !ch.closed {
		t.Fatal("expected channel to be closed even after failure")
	}
	if err != nil && err.Error() != "exchange declare failed" {
		t.Fatalf("expected exchange declare failed, got: %v", err)
	}
}

// ─────────────────────────────────────────────
// Tests: WatchConnAndRetry topology replay
// ─────────────────────────────────────────────

// 異常斷線重連後，有 topology → 應自動 replay
func TestWatchConnAndRetry_ReplayTopologyAfterReconnect(t *testing.T) {
	firstMock := newMockConn()
	secondMock := newMockConn()
	callCount := 0

	// 記錄 ExchangeDeclare 是否被呼叫，用來確認 replay 有執行
	exchangeDeclareCount := 0
	secondMock.channelFunc = func() (AMQPChannel, error) {
		return &mockChannel{
			exchangeDeclareFunc: func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
				exchangeDeclareCount++
				return nil
			},
		}, nil
	}

	cm := newTestConnectionManager()
	cm.dialFunc = func() (AMQPConnection, error) {
		callCount++
		if callCount == 1 {
			return firstMock, nil
		}
		return secondMock, nil
	}

	// 預先存入 topology，模擬已經呼叫過 InitTopology
	topology := newTestTopology()
	cm.topology.Store(&topology)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cm.WatchConnAndRetry(ctx) }()

	// 確保 WatchConnAndRetry 已開始監聽 firstMock 的 closeCh
	time.Sleep(100 * time.Millisecond)
	firstMock.simulateUnexpectedClose(320, "connection reset")

	// 確保重連和 replay 都完成後再 cancel
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil after reconnect and replay, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not return after cancel")
	}

	if exchangeDeclareCount == 0 {
		t.Fatal("expected topology to be replayed after reconnect, but ExchangeDeclare was not called")
	}
}

// 異常斷線重連後，replay 失敗 → WatchConnAndRetry 應回傳 error
func TestWatchConnAndRetry_ReplayTopologyFails(t *testing.T) {
	firstMock := newMockConn()
	secondMock := newMockConn()
	callCount := 0

	secondMock.channelFunc = func() (AMQPChannel, error) {
		return &mockChannel{
			exchangeDeclareFunc: func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
				return errors.New("exchange declare failed")
			},
		}, nil
	}

	cm := newTestConnectionManager()
	cm.dialFunc = func() (AMQPConnection, error) {
		callCount++
		if callCount == 1 {
			return firstMock, nil
		}
		return secondMock, nil
	}

	topology := newTestTopology()
	cm.topology.Store(&topology)

	done := make(chan error, 1)
	go func() { done <- cm.WatchConnAndRetry(context.Background()) }()

	time.Sleep(100 * time.Millisecond)
	firstMock.simulateUnexpectedClose(320, "connection reset")

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error when topology replay fails")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not return after replay failure")
	}
}

// 異常斷線重連後，沒有 topology → 不應嘗試 replay，正常繼續監聽
func TestWatchConnAndRetry_NoTopologyNoReplay(t *testing.T) {
	firstMock := newMockConn()
	secondMock := newMockConn()
	callCount := 0

	// secondMock 故意不設 channelFunc
	// 如果有嘗試 replay，Channel() 會回傳 "not implemented" error
	// WatchConnAndRetry 就會回傳 error，cancel() 後的 nil 檢查就會失敗
	cm := newTestConnectionManager()
	cm.dialFunc = func() (AMQPConnection, error) {
		callCount++
		if callCount == 1 {
			return firstMock, nil
		}
		return secondMock, nil
	}

	// 不存入 topology，cm.topology.Load() 會回傳 nil
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cm.WatchConnAndRetry(ctx) }()

	time.Sleep(100 * time.Millisecond)
	firstMock.simulateUnexpectedClose(320, "connection reset")

	// 給重連時間完成，確認沒有觸發 replay
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil when no topology, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not return after cancel")
	}
}
