package rabbitmq

import (
	"errors"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ─────────────────────────────────────────────
// mockDeferredConfirmation：實作 AMQPDeferredConfirmation interface
// ─────────────────────────────────────────────

type mockDeferredConfirmation struct {
	confirmed bool
}

func (m *mockDeferredConfirmation) Wait() bool {
	return m.confirmed
}

var _ AMQPDeferredConfirmation = (*mockDeferredConfirmation)(nil)

// ─────────────────────────────────────────────
// 擴充 mockChannel，補上 Confirm 和 PublishWithDeferredConfirm
// ─────────────────────────────────────────────
// 注意：mockChannel struct 本體定義在 topology_test.go
// 這裡只補上 producer 需要的方法

func (m *mockChannel) Confirm(noWait bool) error {
	if m.confirmFunc != nil {
		return m.confirmFunc(noWait)
	}
	return nil
}

func (m *mockChannel) PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (AMQPDeferredConfirmation, error) {
	if m.publishWithDeferredConfirmFunc != nil {
		return m.publishWithDeferredConfirmFunc(exchange, key, mandatory, immediate, msg)
	}
	return &mockDeferredConfirmation{confirmed: true}, nil
}

// ─────────────────────────────────────────────
// Tests: PublishWithRetry()
// ─────────────────────────────────────────────

// 正常流程：publish 成功且 broker confirm
func TestPublishWithRetry_Success(t *testing.T) {
	ch := &mockChannel{}
	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	err := cm.PublishWithRetry(t.Context(), "test.exchange", "key.1", []byte("hello"), 1, 1*time.Second)

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

// connect() 失敗時，應回傳 error
func TestPublishWithRetry_ConnectFails(t *testing.T) {
	cm := newTestConnectionManager() // conn 為 nil，沒有 broker

	err := cm.PublishWithRetry(t.Context(), "test.exchange", "key.1", []byte("hello"), 1, 1*time.Second)

	if err == nil {
		t.Fatal("expected error when connect fails")
	}
}

// Channel() 失敗時，應回傳 error
func TestPublishWithRetry_ChannelFails(t *testing.T) {
	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannelError(errors.New("channel open failed"))

	err := cm.PublishWithRetry(t.Context(), "test.exchange", "key.1", []byte("hello"), 1, 1*time.Second)

	if err == nil {
		t.Fatal("expected error when channel fails")
	}
}

// Confirm() 失敗時，應回傳 error
func TestPublishWithRetry_ConfirmFails(t *testing.T) {
	ch := &mockChannel{
		confirmFunc: func(noWait bool) error {
			return errors.New("confirm mode failed")
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	err := cm.PublishWithRetry(t.Context(), "test.exchange", "key.1", []byte("hello"), 1, 1*time.Second)

	if err == nil {
		t.Fatal("expected error when confirm fails")
	}
}

// PublishWithDeferredConfirm() 失敗時，應回傳 error
func TestPublishWithRetry_PublishFails(t *testing.T) {
	ch := &mockChannel{
		publishWithDeferredConfirmFunc: func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (AMQPDeferredConfirmation, error) {
			return nil, errors.New("publish failed")
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	err := cm.PublishWithRetry(t.Context(), "test.exchange", "key.1", []byte("hello"), 1, 1*time.Second)

	if err == nil {
		t.Fatal("expected error when publish fails")
	}
}

// broker 沒有 confirm（Wait() 回傳 false），應回傳 error
func TestPublishWithRetry_NotConfirmed(t *testing.T) {
	ch := &mockChannel{
		publishWithDeferredConfirmFunc: func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (AMQPDeferredConfirmation, error) {
			return &mockDeferredConfirmation{confirmed: false}, nil
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	err := cm.PublishWithRetry(t.Context(), "test.exchange", "key.1", []byte("hello"), 1, 1*time.Second)

	if err == nil {
		t.Fatal("expected error when broker does not confirm")
	}
}

// ─────────────────────────────────────────────
// Tests: 錯誤攜帶堆疊
// ─────────────────────────────────────────────

// broker 沒 confirm 的錯誤誕生在 rabbitmq 內，必須帶堆疊
func TestPublishWithRetry_NotConfirmed_CarriesStack(t *testing.T) {
	ch := &mockChannel{
		publishWithDeferredConfirmFunc: func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (AMQPDeferredConfirmation, error) {
			return &mockDeferredConfirmation{confirmed: false}, nil
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	err := cm.PublishWithRetry(t.Context(), "test.exchange", "key.1", []byte("hello"), 1, 1*time.Second)

	assertCarriesStack(t, err, "rabbitmq.(*ConnectionManager).PublishWithRetry")
}

// amqp091 回傳的外部錯誤自身沒有堆疊，必須在 rabbitmq 邊界補上
func TestPublishWithRetry_ExternalErrorsCarryStack(t *testing.T) {
	tests := []struct {
		name string
		conn AMQPConnection
	}{
		{
			name: "Channel 失敗",
			conn: newMockConnWithChannelError(errors.New("channel open failed")),
		},
		{
			name: "Confirm 失敗",
			conn: newMockConnWithChannel(&mockChannel{
				confirmFunc: func(noWait bool) error {
					return errors.New("confirm mode failed")
				},
			}),
		},
		{
			name: "PublishWithDeferredConfirm 失敗",
			conn: newMockConnWithChannel(&mockChannel{
				publishWithDeferredConfirmFunc: func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (AMQPDeferredConfirmation, error) {
					return nil, errors.New("publish failed")
				},
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := newTestConnectionManager()
			cm.conn = tt.conn

			err := cm.PublishWithRetry(t.Context(), "test.exchange", "key.1", []byte("hello"), 1, 1*time.Second)

			assertCarriesStack(t, err, "rabbitmq.(*ConnectionManager).PublishWithRetry")
		})
	}
}

// 驗證 publish 時傳入的參數正確
func TestPublishWithRetry_Params(t *testing.T) {
	var capturedExchange, capturedKey string
	var capturedBody []byte

	ch := &mockChannel{
		publishWithDeferredConfirmFunc: func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (AMQPDeferredConfirmation, error) {
			capturedExchange = exchange
			capturedKey = key
			capturedBody = msg.Body
			return &mockDeferredConfirmation{confirmed: true}, nil
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	err := cm.PublishWithRetry(t.Context(), "test.exchange", "key.1", []byte("hello"), 1, 1*time.Second)

	if capturedExchange != "test.exchange" {
		t.Errorf("expected exchange %q, got %q", "test.exchange", capturedExchange)
	}
	if capturedKey != "key.1" {
		t.Errorf("expected key %q, got %q", "key.1", capturedKey)
	}
	if string(capturedBody) != "hello" {
		t.Errorf("expected body %q, got %q", "hello", capturedBody)
	}
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
}

// 驗證 publish 失敗後會 retry，成功後停止
func TestPublishWithRetry_RetryOnFailure(t *testing.T) {
	callCount := 0

	ch := &mockChannel{
		publishWithDeferredConfirmFunc: func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (AMQPDeferredConfirmation, error) {
			callCount++
			if callCount < 3 {
				return nil, errors.New("temporary failure")
			}
			return &mockDeferredConfirmation{confirmed: true}, nil
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	err := cm.PublishWithRetry(t.Context(), "test.exchange", "key.1", []byte("hello"), 5, 5*time.Second)

	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if callCount != 3 {
		t.Errorf("expected 3 publish attempts, got %d", callCount)
	}
}

// 驗證成功後 channel 被 Close()
func TestPublishWithRetry_ChannelClosedAfterSuccess(t *testing.T) {
	ch := &mockChannel{}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	err := cm.PublishWithRetry(t.Context(), "test.exchange", "key.1", []byte("hello"), 1, 1*time.Second)

	if !ch.closed {
		t.Fatal("expected channel to be closed after publish")
	}
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

// 驗證失敗後 channel 也會被 Close()（defer 保證）
func TestPublishWithRetry_ChannelClosedAfterFailure(t *testing.T) {
	ch := &mockChannel{
		confirmFunc: func(noWait bool) error {
			return errors.New("confirm mode failed")
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	err := cm.PublishWithRetry(t.Context(), "test.exchange", "key.1", []byte("hello"), 1, 1*time.Second)

	if !ch.closed {
		t.Fatal("expected channel to be closed even after failure")
	}
	// 錯誤在邊界會被 eris.Wrap 補上堆疊與情境，訊息裡仍要看得到底層的原因
	if err != nil && !strings.Contains(err.Error(), "confirm mode failed") {
		t.Fatalf("expected confirm mode failed, got: %v", err)
	}
}
