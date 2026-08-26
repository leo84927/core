package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/leo84927/core/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ─────────────────────────────────────────────
// mockDelivery：實作 AMQPDelivery interface
// ─────────────────────────────────────────────

type mockDelivery struct {
	ackFunc        func(multiple bool) error
	nackFunc       func(multiple bool, requeue bool) error
	ackCalled      bool
	nackCalled     bool
	requeuedWith   bool
	nackedMultiple bool
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
	m.nackedMultiple = multiple
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
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) error {
		return nil
	})

	if err == nil {
		t.Fatal("expected error when connect fails")
	}
}

// Channel() 失敗時，應回傳 error
func TestWaitForConsume_ChannelFails(t *testing.T) {
	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannelError(errors.New("channel open failed"))

	consumer := newTestConsumer(cm)
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) error {
		return nil
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
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) error {
		return nil
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
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) error {
		return nil
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
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) error {
		return nil
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
		done <- consumer.WaitForConsume(ctx, func(ctx context.Context, msg Message, _ PublishHandler) error {
			return nil
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
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) error {
		return nil
	})

	if !ch.closed {
		t.Fatal("expected channel to be closed after WaitForConsume")
	}
	if err != nil && err.Error() != "channel closed" {
		t.Fatalf("expected channel closed, got: %v", err)
	}
}

// ─────────────────────────────────────────────
// Tests: 錯誤攜帶堆疊
// ─────────────────────────────────────────────

// consumer 中斷（msgs channel 關閉）是 Grafana 上最常見的那則錯誤，必須自誕生起就帶堆疊
func TestWaitForConsume_MsgsChannelClosed_CarriesStack(t *testing.T) {
	ch := &mockChannel{
		consumeFunc: func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			return deliveryChannel(), nil // 空的且已關閉
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	consumer := newTestConsumer(cm)
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) error {
		return nil
	})

	assertCarriesStack(t, err, "rabbitmq.(*Consumer).subscribeAndWait")
}

/*
 * 把 rabbitmq 的錯誤與 core 的錯誤日誌入口串起來：consumer 中斷的錯誤經 logger.Error 記錄後
 * exception.stacktrace 必須看得到 rabbitmq 套件內的框，否則就是「錯誤有堆疊但日誌沒帶到」
 */
func TestWaitForConsume_MsgsChannelClosed_StacktraceReachesLog(t *testing.T) {
	var buf bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(original) })

	ch := &mockChannel{
		consumeFunc: func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			return deliveryChannel(), nil // 空的且已關閉
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	consumer := newTestConsumer(cm)
	err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) error {
		return nil
	})

	logger.Error(context.Background(), "consumer interrupted", err)

	// slog.SetDefault 會把 log 套件的輸出也導進同一個 handler，所以 buffer 裡不只一筆，取帶堆疊的那筆
	var stacktrace string
	decoder := json.NewDecoder(&buf)
	for decoder.More() {
		var record struct {
			Stacktrace string `json:"exception.stacktrace"`
		}
		if err := decoder.Decode(&record); err != nil {
			t.Fatalf("解析日誌失敗：%v", err)
		}
		if record.Stacktrace != "" {
			stacktrace = record.Stacktrace
		}
	}

	if !strings.Contains(stacktrace, "rabbitmq.(*Consumer).subscribeAndWait") {
		t.Errorf("exception.stacktrace =\n%s\n期望看得到 rabbitmq 套件內的框", stacktrace)
	}
}

// amqp091 回傳的外部錯誤自身沒有堆疊，必須在 rabbitmq 邊界補上，否則堆疊裡看不到任何 rabbitmq 的框
func TestWaitForConsume_ExternalErrorsCarryStack(t *testing.T) {
	tests := []struct {
		name string
		conn AMQPConnection
	}{
		{
			name: "Channel 失敗",
			conn: newMockConnWithChannelError(errors.New("channel open failed")),
		},
		{
			name: "Qos 失敗",
			conn: newMockConnWithChannel(&mockChannel{
				qosFunc: func(prefetchCount, prefetchSize int, global bool) error {
					return errors.New("qos failed")
				},
			}),
		},
		{
			name: "Consume 失敗",
			conn: newMockConnWithChannel(&mockChannel{
				consumeFunc: func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
					return nil, errors.New("consume failed")
				},
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := newTestConnectionManager()
			cm.conn = tt.conn

			consumer := newTestConsumer(cm)
			err := consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) error {
				return nil
			})

			assertCarriesStack(t, err, "rabbitmq.(*Consumer).subscribeAndWait")
		})
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

	consumer.handleDelivery(context.Background(), d, Message{Body: []byte("hello")}, nil, func(ctx context.Context, msg Message, _ PublishHandler) error {
		return nil
	})

	if !d.ackCalled {
		t.Fatal("expected Ack to be called on success")
	}
	if d.nackCalled {
		t.Fatal("expected Nack not to be called on success")
	}
}

/*
 * handler 失敗時一律 Nack(false, false)：core 不做錯誤分類，分不出暫時性與永久性失敗，
 * requeue=true 對永久性失敗就是無限迴圈
 */
func TestHandleDelivery_HandlerFails_NacksWithoutRequeue(t *testing.T) {
	d := &mockDelivery{}
	cm := newTestConnectionManager()
	consumer := newTestConsumer(cm)

	consumer.handleDelivery(context.Background(), d, Message{Body: []byte("hello")}, nil, func(ctx context.Context, msg Message, _ PublishHandler) error {
		return errors.New("handler failed")
	})

	if d.ackCalled {
		t.Fatal("expected Ack not to be called on failure")
	}
	if !d.nackCalled {
		t.Fatal("expected Nack to be called on failure")
	}
	if d.requeuedWith {
		t.Fatal("expected Nack to be called with requeue=false")
	}
	if d.nackedMultiple {
		t.Fatal("expected Nack to be called with multiple=false，per-message goroutine 併發確認不可批次")
	}
}

/*
 * worker panic 只能讓「該則訊息」失敗：per-message goroutine 若讓 panic 逃出去，
 * 整個 process 會被帶走，其餘 in-flight 的訊息一起陪葬
 */
func TestHandleDelivery_HandlerPanics_NacksAndSurvives(t *testing.T) {
	d := &mockDelivery{}
	cm := newTestConnectionManager()
	consumer := newTestConsumer(cm)

	consumer.handleDelivery(context.Background(), d, Message{Body: []byte("hello")}, nil, func(ctx context.Context, msg Message, _ PublishHandler) error {
		panic("boom")
	})

	if d.ackCalled {
		t.Fatal("expected Ack not to be called on panic")
	}
	if !d.nackCalled {
		t.Fatal("expected Nack to be called on panic")
	}
	if d.requeuedWith {
		t.Fatal("expected Nack to be called with requeue=false on panic")
	}
}

// panic 的訊息落到下一則訊息上就等於錯殺，panic 之後 consumer 必須照常處理後續訊息
func TestHandleDelivery_PanicDoesNotAffectNextMessage(t *testing.T) {
	cm := newTestConnectionManager()
	consumer := newTestConsumer(cm)

	panicked := &mockDelivery{}
	consumer.handleDelivery(context.Background(), panicked, Message{Body: []byte("bad")}, nil, func(ctx context.Context, msg Message, _ PublishHandler) error {
		panic("boom")
	})

	next := &mockDelivery{}
	consumer.handleDelivery(context.Background(), next, Message{Body: []byte("good")}, nil, func(ctx context.Context, msg Message, _ PublishHandler) error {
		return nil
	})

	if !next.ackCalled {
		t.Fatal("expected the message after a panic to still be acked")
	}
}

// panic 轉成的錯誤必須帶得到 panic 現場，否則 Grafana 上只看得到「recovered」四個字
func TestHandleDelivery_PanicErrorCarriesPanicSite(t *testing.T) {
	cm := newTestConnectionManager()
	consumer := newTestConsumer(cm)

	err := consumer.runHandler(context.Background(), Message{}, func(ctx context.Context, msg Message, _ PublishHandler) error {
		panic("boom")
	})

	if err == nil {
		t.Fatal("expected panic to be converted into an error")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("err = %q，期望帶上 panic 的值", err.Error())
	}
	assertCarriesStack(t, err, "rabbitmq.(*Consumer).runHandler")
}

/*
 * 關機途中的失敗不可否認：沒有 DLX 時 Nack(false, false) 等於把訊息刪掉，
 * 而 ctx 取消造成的失敗是「還沒做完」不是「做了但失敗」——
 * 這樣每一次關機都會打破一次 at-least-once。留著不確認，broker 才會在 channel 關閉時重新入隊
 */
func TestHandleDelivery_HandlerFailsDuringShutdown_LeavesMessageUnacked(t *testing.T) {
	d := &mockDelivery{}
	cm := newTestConnectionManager()
	consumer := newTestConsumer(cm)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	consumer.handleDelivery(ctx, d, Message{Body: []byte("hello")}, nil, func(ctx context.Context, msg Message, _ PublishHandler) error {
		return ctx.Err()
	})

	if d.nackCalled {
		t.Fatal("關機途中的失敗被 Nack 掉了：沒有 DLX 時這則訊息就此消失，broker 不會重送")
	}
	if d.ackCalled {
		t.Fatal("工作沒做完不該 Ack")
	}
}

// 工作做完之後才收到取消，仍然要 Ack —— 否則已經做完的工作會被重送，白白多執行一次
func TestHandleDelivery_HandlerSucceedsDuringShutdown_StillAcks(t *testing.T) {
	d := &mockDelivery{}
	cm := newTestConnectionManager()
	consumer := newTestConsumer(cm)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	consumer.handleDelivery(ctx, d, Message{Body: []byte("hello")}, nil, func(ctx context.Context, msg Message, _ PublishHandler) error {
		return nil
	})

	if !d.ackCalled {
		t.Fatal("工作已完成，即使 ctx 已取消也要 Ack")
	}
}

// handler 收到的 Message.Body 應和 delivery 的一致
func TestHandleDelivery_PassesCorrectBody(t *testing.T) {
	d := &mockDelivery{}
	cm := newTestConnectionManager()
	consumer := newTestConsumer(cm)

	var receivedBody []byte
	consumer.handleDelivery(context.Background(), d, Message{Body: []byte("test-body")}, nil, func(ctx context.Context, msg Message, _ PublishHandler) error {
		receivedBody = msg.Body
		return nil
	})

	if string(receivedBody) != "test-body" {
		t.Errorf("expected body %q, got %q", "test-body", string(receivedBody))
	}
}

// ─────────────────────────────────────────────
// Tests: 併發與確認契約
// ─────────────────────────────────────────────

/*
 * prefetch 是這個系統唯一的併發度旋鈕，寫死在 core 而非設定鍵，三個 consumer 服務同值。
 * 之前 handler 立刻回傳成功，未確認訊息數永遠是 0，prefetch 從未真的生效
 */
func TestWaitForConsume_SetsPrefetchToFour(t *testing.T) {
	var gotCount, gotSize int
	var gotGlobal bool

	ch := &mockChannel{
		qosFunc: func(prefetchCount, prefetchSize int, global bool) error {
			gotCount, gotSize, gotGlobal = prefetchCount, prefetchSize, global
			return nil
		},
		consumeFunc: func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			return deliveryChannel(), nil
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	consumer := newTestConsumer(cm)
	_ = consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) error {
		return nil
	})

	if gotCount != 4 {
		t.Errorf("prefetchCount = %d, 期望 4", gotCount)
	}
	if gotSize != 0 {
		t.Errorf("prefetchSize = %d, 期望 0（不限制大小）", gotSize)
	}
	if gotGlobal {
		t.Error("期望 global=false，只對當前 channel 生效")
	}
}

/*
 * work-then-Ack 的另一半：訊息不可以擋住 for/select，否則未確認訊息數永遠是 1，
 * prefetch 這個旋鈕還是沒有意義
 */
func TestWaitForConsume_DispatchesMessagesConcurrently(t *testing.T) {
	const messages = 3

	ch := &mockChannel{
		consumeFunc: func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			return deliveryChannel(make([]amqp.Delivery, messages)...), nil
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	// 每個 handler 都等到 messages 個 handler 都進來才放行；序列執行的話這裡會死鎖到逾時
	entered := make(chan struct{}, messages)
	release := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		consumer := newTestConsumer(cm)
		_ = consumer.WaitForConsume(context.Background(), func(ctx context.Context, msg Message, _ PublishHandler) error {
			entered <- struct{}{}
			<-release
			return nil
		})
	}()

	for range messages {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("訊息沒有併發處理：handler 仍然擋住 for/select")
		}
	}
	close(release)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForConsume 沒有結束")
	}
}

/*
 * drain：ctx 取消之後仍要等 in-flight 的 goroutine 把工作做完，
 * 且必須在 AMQP channel 關閉「之前」等完 —— 否則 Ack 打在已關閉的 channel 上，訊息一律被重送，等於白等
 */
func TestWaitForConsume_DrainsInFlightBeforeClosingChannel(t *testing.T) {
	msgs := make(chan amqp.Delivery, 1)
	msgs <- amqp.Delivery{Body: []byte("slow")}

	ready := make(chan struct{})
	ch := &mockChannel{
		consumeFunc: func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			return msgs, nil
		},
	}

	cm := newTestConnectionManager()
	cm.conn = newMockConnWithChannel(ch)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	var finished bool
	var closedWhileRunning bool

	go func() {
		defer close(done)
		consumer := newTestConsumer(cm)
		_ = consumer.WaitForConsume(ctx, func(ctx context.Context, msg Message, _ PublishHandler) error {
			close(ready)
			time.Sleep(100 * time.Millisecond)
			closedWhileRunning = ch.closed
			finished = true
			return nil
		})
	}()

	// 確保 handler 已經開始，再送出關機訊號
	<-ready
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForConsume 沒有結束")
	}

	if !finished {
		t.Fatal("關機時沒有等 in-flight 的訊息做完")
	}
	if closedWhileRunning {
		t.Fatal("AMQP channel 在 drain 完成前就被關閉，in-flight 的 Ack 會打在已關閉的 channel 上")
	}
	if !ch.closed {
		t.Fatal("drain 之後仍應關閉 AMQP channel")
	}
}
