package rabbitmq

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/cenkalti/backoff/v5"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rotisserie/eris"
)

// ─────────────────────────────────────────────
// Helper
// ─────────────────────────────────────────────

// newMockConnWithChannelError 建立一個開 channel 就失敗的連線
func newMockConnWithChannelError(err error) *mockConn {
	mock := newMockConn()
	mock.channelFunc = func() (AMQPChannel, error) {
		return nil, err
	}
	return mock
}

/*
 * assertCarriesStack 驗證錯誤自 rabbitmq 內部誕生起就帶著堆疊
 * 堆疊只能在 eris.New / eris.Wrap 當下擷取，事後補救只會抓到記錄日誌那一行，對除錯沒有價值
 * wantFrame 是期望出現在堆疊裡的框，格式同 eris 的 package.func
 */
func assertCarriesStack(t *testing.T, err error, wantFrame string) {
	t.Helper()

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if len(eris.StackFrames(err)) == 0 {
		t.Fatalf("錯誤不帶 eris 堆疊，Grafana 的 exception.stacktrace 會只剩一行訊息：%v", err)
	}

	trace := eris.ToString(err, true)
	if !strings.Contains(trace, wantFrame) {
		t.Errorf("堆疊 =\n%s\n期望含 rabbitmq 套件內的框 %q", trace, wantFrame)
	}
}

// ─────────────────────────────────────────────
// Tests: permanentIfNeeded()
// ─────────────────────────────────────────────

// 不可重試的 amqp 錯誤：分類歸分類，回傳的必須還是原本那條錯誤鏈，不能換成 errors.As 取出的底層錯誤
func TestPermanentIfNeeded_KeepsChainForPermanentAmqpError(t *testing.T) {
	wrapped := eris.Wrap(&amqp.Error{Code: amqp.AccessRefused, Reason: "access refused"}, "dial amqp failed")

	got := permanentIfNeeded(wrapped)

	var permanent *backoff.PermanentError
	if !errors.As(got, &permanent) {
		t.Fatalf("期望不可重試的錯誤被 backoff.Permanent 包裝，got: %v", got)
	}

	inner := permanent.Unwrap()
	if len(eris.StackFrames(inner)) == 0 {
		t.Error("Permanent 內的錯誤不帶堆疊，代表包裝鏈被丟掉了")
	}
	if amqpErr := (*amqp.Error)(nil); !errors.As(inner, &amqpErr) {
		t.Error("期望仍能用 errors.As 取出 *amqp.Error")
	}
	if !strings.Contains(inner.Error(), "dial amqp failed") {
		t.Errorf("錯誤訊息 = %q, 期望保留原本的包裝訊息", inner.Error())
	}
}

// 可重試的 amqp 錯誤同樣要原樣回傳，讓堆疊跟著重試路徑一起往上
func TestPermanentIfNeeded_KeepsChainForRetryableAmqpError(t *testing.T) {
	wrapped := eris.Wrap(&amqp.Error{Code: amqp.ConnectionForced, Reason: "connection forced"}, "dial amqp failed")

	got := permanentIfNeeded(wrapped)

	if len(eris.StackFrames(got)) == 0 {
		t.Error("回傳的錯誤不帶堆疊，代表包裝鏈被丟掉了")
	}
	if amqpErr := (*amqp.Error)(nil); !errors.As(got, &amqpErr) {
		t.Error("期望仍能用 errors.As 取出 *amqp.Error")
	}
	if !strings.Contains(got.Error(), "dial amqp failed") {
		t.Errorf("錯誤訊息 = %q, 期望保留原本的包裝訊息", got.Error())
	}
}

// URL 格式錯誤同樣不可被拆解到底層的 *url.Error
func TestPermanentIfNeeded_KeepsChainForUrlError(t *testing.T) {
	urlErr := &url.Error{Op: "parse", URL: "amqp://host", Err: errors.New("invalid character")}
	wrapped := eris.Wrap(urlErr, "dial amqp failed")

	got := permanentIfNeeded(wrapped)

	var permanent *backoff.PermanentError
	if !errors.As(got, &permanent) {
		t.Fatalf("期望 URL 格式錯誤被 backoff.Permanent 包裝，got: %v", got)
	}

	inner := permanent.Unwrap()
	if len(eris.StackFrames(inner)) == 0 {
		t.Error("Permanent 內的錯誤不帶堆疊，代表包裝鏈被丟掉了")
	}
	if got := (*url.Error)(nil); !errors.As(inner, &got) {
		t.Error("期望仍能用 errors.As 取出 *url.Error")
	}
}

// 其他可重試的錯誤原樣回傳
func TestPermanentIfNeeded_KeepsChainForOtherError(t *testing.T) {
	wrapped := eris.Wrap(errors.New("connection reset by peer"), "dial amqp failed")

	got := permanentIfNeeded(wrapped)

	if got != wrapped {
		t.Errorf("got = %v, 期望原樣回傳同一個錯誤實例", got)
	}
}

// nil 依然回傳 nil
func TestPermanentIfNeeded_NilStaysNil(t *testing.T) {
	if got := permanentIfNeeded(nil); got != nil {
		t.Errorf("got = %v, 期望 nil", got)
	}
}

// ─────────────────────────────────────────────
// Tests: unwrapPermanent()
// ─────────────────────────────────────────────

/*
 * backoff.Retry 只有在「還沒用完重試次數」時才會自己解開 Permanent 包裝
 * 不可重試的錯誤若剛好落在最後一次嘗試，回傳的最外層會是 *backoff.PermanentError（非 eris 型別）
 * eris 只認得最外層，屆時堆疊會整條看不到，所以離開 rabbitmq 前必須自己解開
 */
func TestUnwrapPermanent_ExposesErisStackAgain(t *testing.T) {
	wrapped := eris.Wrap(&amqp.Error{Code: amqp.AccessRefused, Reason: "access refused"}, "dial amqp failed")

	got := unwrapPermanent(backoff.Permanent(wrapped))

	if permanent := (*backoff.PermanentError)(nil); errors.As(got, &permanent) {
		t.Error("期望 backoff 的 Permanent 包裝已被解開，不要往外傳")
	}
	if len(eris.StackFrames(got)) == 0 {
		t.Errorf("錯誤不帶 eris 堆疊：%v", got)
	}
}

// 不是 Permanent 包裝時原樣回傳
func TestUnwrapPermanent_KeepsOtherErrors(t *testing.T) {
	err := eris.New("channel closed")

	if got := unwrapPermanent(err); got != err {
		t.Errorf("got = %v, 期望原樣回傳同一個錯誤實例", got)
	}
	if got := unwrapPermanent(nil); got != nil {
		t.Errorf("got = %v, 期望 nil", got)
	}
}

// 從 connect() 出來的錯誤不可還帶著 backoff 的 Permanent 包裝，否則 logger 端看不到堆疊
func TestConnect_PermanentDialError_CarriesStack(t *testing.T) {
	cm := newTestConnectionManager() // MaxRetries=1，第一次嘗試就用完重試次數
	cm.dialFunc = func() (AMQPConnection, error) {
		// 模擬 broker 拒絕帳密：buildConnection 會先包上堆疊，再由 permanentIfNeeded 判為不可重試
		return nil, permanentIfNeeded(eris.Wrap(&amqp.Error{Code: amqp.AccessRefused, Reason: "access refused"}, "dial amqp failed"))
	}

	_, err := cm.connect(context.Background())
	if err == nil {
		t.Fatal("expected error when dial is refused")
	}

	if permanent := (*backoff.PermanentError)(nil); errors.As(err, &permanent) {
		t.Error("期望 backoff 的 Permanent 包裝不要外流到呼叫端")
	}
	if len(eris.StackFrames(err)) == 0 {
		t.Errorf("錯誤不帶 eris 堆疊：%v", err)
	}
	if amqpErr := (*amqp.Error)(nil); !errors.As(err, &amqpErr) {
		t.Error("期望仍能用 errors.As 取出 *amqp.Error")
	}
}
