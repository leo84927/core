package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type mockConn struct {
	mu          sync.Mutex
	closed      bool
	closeCh     chan *amqp.Error
	channelFunc func() (AMQPChannel, error)
}

func (m *mockConn) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockConn) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCh = c
	return c
}

func (m *mockConn) Channel() (AMQPChannel, error) {
	if m.channelFunc != nil {
		return m.channelFunc()
	}
	return nil, errors.New("not implemented")
}

func newMockConn() *mockConn {
	return &mockConn{
		closeCh: make(chan *amqp.Error, 1),
	}
}

// 模擬 broker 異常斷線
func (m *mockConn) simulateUnexpectedClose(code int, reason string) {
	m.mu.Lock()
	m.closed = true
	ch := m.closeCh
	m.mu.Unlock()
	ch <- &amqp.Error{Code: code, Reason: reason}
}

// 模擬正常關閉
func (m *mockConn) simulateNormalClose() {
	m.mu.Lock()
	m.closed = true
	ch := m.closeCh
	m.mu.Unlock()
	close(ch)
}

// ─────────────────────────────────────────────
// Helper
// ─────────────────────────────────────────────

func newTestConnectionManager() *ConnectionManager {
	return NewConnectionManager(&Config{
		MaxElapsedTime: 1 * time.Second,
		MaxRetries:     1,
	})
}

// ─────────────────────────────────────────────
// Tests: 驗證 mockConn 確實滿足 AMQPConnection interface（編譯期檢查）
// ─────────────────────────────────────────────

var _ AMQPConnection = (*mockConn)(nil)

// ─────────────────────────────────────────────
// Tests: connect()
// ─────────────────────────────────────────────

// 已有活躍連線，不應重新建立
func TestConnect_ReusesExistingConn(t *testing.T) {
	cm := newTestConnectionManager()
	cm.conn = newMockConn() // 直接塞入 mock，不需要真實連線

	conn, err := cm.connect(t.Context())
	if conn != cm.conn {
		t.Fatal("expected same connection instance to be reused")
	}
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

// conn 為 nil，setConnWithRetry 失敗時應回傳 error
func TestConnect_FailsWhenNoConn(t *testing.T) {
	cm := newTestConnectionManager() // cm.conn 為 nil，會觸發 setConnWithRetry

	conn, err := cm.connect(t.Context())
	if conn != nil {
		t.Fatalf("expected no connection, got: %v", conn)
	}
	if err == nil {
		t.Fatal("expected error when no broker available, got nil")
	}
}

// conn 已關閉，應視為無效並嘗試重建
// 這裡驗證的是：IsClosed() == true 時不會直接回傳舊連線
func TestConnect_DoesNotReuseClosedConn(t *testing.T) {
	cm := newTestConnectionManager()
	closedConn := newMockConn()
	closedConn.closed = true
	cm.conn = closedConn

	// 沒有 broker，setConnWithRetry 會失敗
	conn, err := cm.connect(t.Context())
	if conn != nil {
		t.Fatalf("expected no connection, got: %v", conn)
	}
	if err == nil {
		t.Fatal("expected error: closed conn should trigger redial, which fails without broker")
	}
}

// ─────────────────────────────────────────────
// Tests: getConn()
// ─────────────────────────────────────────────

// conn 為 nil 時，getConn() 應回傳 error
func TestGetConn_NoConnection(t *testing.T) {
	cm := newTestConnectionManager()

	conn, err := cm.getConn()
	if conn != nil {
		t.Fatalf("expected no connection, got: %v", conn)
	}
	if err == nil {
		t.Fatal("expected error when conn is nil")
	}
}

// conn 存在且可用時，getConn() 應回傳該 conn
func TestGetConn_WithActiveConn(t *testing.T) {
	cm := newTestConnectionManager()
	cm.conn = newMockConn()

	conn, err := cm.getConn()
	if conn == nil {
		t.Fatal("expected connection, got nil")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// conn 存在但已關閉時，getConn() 應回傳 nil & error
func TestGetConn_ClosedConn(t *testing.T) {
	cm := newTestConnectionManager()
	mock := newMockConn()
	mock.closed = true
	cm.conn = mock

	conn, err := cm.getConn()
	if conn != nil {
		t.Fatalf("expected no connection, got: %v", conn)
	}
	if err == nil {
		t.Fatal("expected error for closed connection")
	}
}

// ─────────────────────────────────────────────
// Tests: 錯誤攜帶堆疊
// ─────────────────────────────────────────────

// 「沒有可用連線」誕生在 rabbitmq 內，必須帶堆疊
func TestGetConn_NoConnection_CarriesStack(t *testing.T) {
	cm := newTestConnectionManager()

	_, err := cm.getConn()

	assertCarriesStack(t, err, "rabbitmq.(*ConnectionManager).getConn")
}

// dial 失敗是 amqp091 的外部錯誤，必須在 buildConnection 當下就補上堆疊
func TestBuildConnection_DialFails_CarriesStack(t *testing.T) {
	// URI 帶空白，amqp091 會在解析階段就失敗，不會真的連到 broker
	config := &Config{
		Host: "invalid host",
		Port: "5672",
	}

	_, err := config.buildConnection()

	assertCarriesStack(t, err, "rabbitmq.(*Config).buildConnection")
}

// ─────────────────────────────────────────────
// Tests: Close()
// ─────────────────────────────────────────────

// 測試正常關閉
func TestClose_ClosesActiveConn(t *testing.T) {
	cm := newTestConnectionManager()
	mock := newMockConn()
	cm.conn = mock

	cm.Close()

	if !mock.IsClosed() {
		t.Fatal("expected connection to be closed after Close()")
	}
}

// 當 Close() 發生 error 時，不應該 panic
func TestClose_NoConnDoesNotPanic(t *testing.T) {
	cm := newTestConnectionManager() // conn 為 nil
	cm.Close()                       // 不應 panic
}

// ─────────────────────────────────────────────
// Tests: WatchConnAndRetry()
// ─────────────────────────────────────────────

// ctx 取消時應正常結束，回傳 nil
func TestWatchConnAndRetry_ContextCancel(t *testing.T) {
	cm := newTestConnectionManager()
	cm.conn = newMockConn()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cm.WatchConnAndRetry(ctx) }()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil on cancel, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		// 保險機制，WatchConnAndRetry 理論上會在 2 秒內回應，超過 2 秒就強制測試失敗
		t.Fatal("WatchConnAndRetry did not return after context cancel")
	}
}

// broker 正常關閉 channel，應回傳 nil
func TestWatchConnAndRetry_NormalClose(t *testing.T) {
	cm := newTestConnectionManager()
	mock := newMockConn()
	cm.conn = mock

	done := make(chan error, 1)
	go func() { done <- cm.WatchConnAndRetry(context.Background()) }()

	/**
	 * 沒加 sleep 的情況
	 * 1. simulateNormalClose() 在 connect() 之前執行，導致 connect() 判斷 conn 已壞 -> 重新 dial
	 * 2. simulateNormalClose() 在 connect() 之後 conn.NotifyClose 之前執行，導致 close 的是舊 channel，監聽的是新 channel -> 永遠收不到關閉訊號，測試逾時
	 * 3. simulateNormalClose() 在 conn.NotifyClose 之後執行 -> 正常流程，測試通過
	 * 4. simulateNormalClose() 在 for select 之後執行 -> 正常流程，測試通過
	 * 加 sleep 讓 simulateNormalClose() 幾乎都在 conn.NotifyClose 之後執行，確保測試穩定通過
	 */
	time.Sleep(100 * time.Millisecond)
	mock.simulateNormalClose()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil on normal close, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WatchConnAndRetry did not return on normal close")
	}
}

// 異常斷線後重連，因為沒有 broker 所以 setConnWithRetry 失敗，應回傳 error
func TestWatchConnAndRetry_UnexpectedClose_ReconnectFails(t *testing.T) {
	cm := newTestConnectionManager()
	mock := newMockConn()
	cm.conn = mock

	done := make(chan error, 1)
	go func() { done <- cm.WatchConnAndRetry(context.Background()) }()

	/**
	 * 沒加 sleep 的情況
	 * 1. simulateUnexpectedClose() 在 connect() 之前執行，導致 connect() 判斷 conn 已壞 -> 持續重試 dial，直到 MaxElpasedTime 或 MaxRetries 達到上限
	 * 2. simulateUnexpectedClose() 在 connect() 之後 conn.NotifyClose 之前執行，導致 close 的是舊 channel，監聽的是新 channel -> 永遠收不到關閉訊號，測試逾時
	 * 3. simulateUnexpectedClose() 在 conn.NotifyClose 之後執行 -> 正常接收 amqpErr，持續重試 dial，直到 MaxElpasedTime 或 MaxRetries 達到上限，測試通過
	 * 4. simulateUnexpectedClose() 在 for select 之後執行 -> 正常接收 amqpErr，持續重試 dial，直到 MaxElpasedTime 或 MaxRetries 達到上限，測試通過
	 * 加 sleep 讓 simulateUnexpectedClose() 幾乎都在 conn.NotifyClose 之後執行，確保測試穩定通過
	 */
	time.Sleep(100 * time.Millisecond)
	mock.simulateUnexpectedClose(500, "internal error")

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after reconnect failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WatchConnAndRetry did not return after reconnect failure")
	}
}

func TestWatchConnAndRetry_UnexpectedClose_ReconnectSucceeds(t *testing.T) {
	firstMock := newMockConn()
	secondMock := newMockConn()
	callCount := 0

	cm := newTestConnectionManager()
	cm.dialFunc = func() (AMQPConnection, error) {
		callCount++
		if callCount == 1 {
			return firstMock, nil // 第一次連線成功
		}
		return secondMock, nil // 重連也成功
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cm.WatchConnAndRetry(ctx) }()

	// 在觸發異常斷線前，確保 WatchConnAndRetry 已開始監聽 firstMock 的 closeCh
	time.Sleep(100 * time.Millisecond)
	firstMock.simulateUnexpectedClose(320, "connection reset")

	// 在正常關閉前，確保 WatchConnAndRetry 已成功重連並監聽 secondMock 的 closeCh
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil after reconnect and cancel, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not return after cancel")
	}
}

// ─────────────────────────────────────────────
// Tests: Concurrent Safety
// ─────────────────────────────────────────────

// 多個 goroutine 同時呼叫 connect()，在 conn 已存在的情況下不應有 race condition
func TestConnect_ConcurrentSafe(t *testing.T) {
	cm := newTestConnectionManager()
	cm.conn = newMockConn()

	var wg sync.WaitGroup
	results := make([]AMQPConnection, 10)

	// 啟動一組 10 個 goroutine 同時呼叫 connect()，並將取得的 conn 寫入 results slice
	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn, err := cm.connect(t.Context())
			if err != nil {
				t.Errorf("goroutine %d: unexpected error: %v", idx, err)
				return
			}
			results[idx] = conn
		}(i)
	}
	wg.Wait()

	// 所有 goroutine 拿到的應該是同一個 conn instance
	for i, conn := range results {
		if conn != cm.conn {
			t.Errorf("goroutine %d got different connection instance", i)
		}
	}
}

// getConn 和 Close 同時呼叫不應 panic 或 race
func TestGetConnAndClose_ConcurrentSafe(t *testing.T) {
	cm := newTestConnectionManager()
	cm.conn = newMockConn()

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := cm.getConn()
			if err != nil && err.Error() != "no active connection" {
				t.Error("expected no active connection, got: ", err)
			}
		}()
		go func() {
			defer wg.Done()
			cm.Close()
		}()
	}
	wg.Wait()
}

// ─────────────────────────────────────────────
// Tests: buildUrl()
// ─────────────────────────────────────────────

func TestBuildUrl(t *testing.T) {
	config := &Config{
		User:     "guest",
		Password: "guest",
		Host:     "localhost",
		Port:     "5672",
		Vhost:    "my_vhost",
	}

	url := config.buildUrl()
	expected := "amqp://guest:guest@localhost:5672/my_vhost"

	if url != expected {
		t.Fatalf("expected %q, got %q", expected, url)
	}
}

func TestBuildUrl_EmptyVhost(t *testing.T) {
	config := &Config{
		User:     "user",
		Password: "pass",
		Host:     "localhost",
		Port:     "5672",
		Vhost:    "",
	}

	url := config.buildUrl()
	expected := "amqp://user:pass@localhost:5672/"

	if url != expected {
		t.Fatalf("expected %q, got %q", expected, url)
	}
}
