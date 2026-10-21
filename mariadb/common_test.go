package mariadb

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/go-sql-driver/mysql"
)

// captureErrorLogs 把 slog 預設 logger 換成寫進記憶體的版本，測完還原；logger.Error 走的就是 slog.Default()
func captureErrorLogs(t *testing.T) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(original) })

	return &buf
}

func isPermanent(err error) bool {
	var permanent *backoff.PermanentError
	return errors.As(err, &permanent)
}

/*
 * 呼叫端已經放棄（ctx 已取消）時，不論驅動回的是哪一種錯誤，都是「還沒做完」而不是「做了但失敗」：
 * 不記 Error —— 關機時每一條還沒暖起來的連線都會走到這裡，那是預期中的收攤。
 * 只動日誌、不動控制流：err 原樣交回 backoff，它在每次 operation 失敗後先問 ctx，結束了就回
 * context.Cause(ctx)；包成 Permanent 反而會讓 backoff 回傳驅動的錯誤，呼叫端拿不到 ctx 錯誤。
 * 用一個不包 ctx 錯誤的 err，釘住「看的是呼叫端的 ctx，不是 err 的型別」。
 */
func TestPermanentIfNeededStaysQuietOnceCallerGivesUp(t *testing.T) {
	logs := captureErrorLogs(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := permanentIfNeeded(ctx, mysql.ErrInvalidConn)

	if isPermanent(err) || !errors.Is(err, mysql.ErrInvalidConn) {
		t.Errorf("permanentIfNeeded() = %#v，期望原樣交回 err，由 backoff 依 ctx 收手", err)
	}
	if logs.Len() > 0 {
		t.Errorf("記了 Error 日誌：%s期望 ctx 取消時不記", logs.String())
	}
}

/*
 * 呼叫端還在等時，網路逾時是可重試的錯誤 —— 即使它 errors.Is(context.DeadlineExceeded)。
 * DSN 的 timeout 到期時，net 套件的撥號在「poller deadline」與「ctx.Done」之間競爭：
 * ctx 先到就回 *net.OpError → *net.timeoutError，它的 Is 對 context.DeadlineExceeded 回 true
 * （實測 200 次中 116 次）。拿 err 的型別判斷「呼叫端放棄了」，會把這些網路逾時當成永久錯誤，
 * 不重試也不留日誌。
 */
func TestPermanentIfNeededRetriesNetworkTimeoutWhileCallerWaits(t *testing.T) {
	logs := captureErrorLogs(t)

	// 用一個已到期的 ctx 撥號，穩定拿到 net 套件那個 Is(context.DeadlineExceeded) 為 true 的逾時錯誤
	expired, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()
	_, dialErr := (&net.Dialer{}).DialContext(expired, "tcp", "127.0.0.1:1")
	if !errors.Is(dialErr, context.DeadlineExceeded) {
		t.Fatalf("前提不成立：撥號錯誤 %v 沒有包住 context.DeadlineExceeded", dialErr)
	}

	err := permanentIfNeeded(context.Background(), dialErr)

	if isPermanent(err) {
		t.Errorf("permanentIfNeeded() 包成了 backoff.Permanent，期望呼叫端還在等時照常重試")
	}
	if logs.Len() == 0 {
		t.Error("沒有記 Error 日誌，期望可重試的網路逾時照常留下紀錄")
	}
}
