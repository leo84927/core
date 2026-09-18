package mariadb

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

/*
 * silentListener 收得下連線但永遠不送 MySQL handshake —— 雲端資料庫睡著時就是這個樣子。
 * connection refused 是瞬回的，量不出「單次連線到底聽不聽 ctx」，只有這種 listener 量得出來。
 */
func silentListener(t *testing.T) (host, port string) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("建立 listener 失敗: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	var mu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})

	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			// 收下就放著：MySQL 的 handshake 由伺服器先送，不送就讓呼叫端一直等
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()

	addr := lis.Addr().(*net.TCPAddr)
	return addr.IP.String(), strconv.Itoa(addr.Port)
}

// silentDSN 指向一個永遠不說話的 listener，DSN 上的每個 timeout 都遠大於測試用的 ctx
func silentDSN(t *testing.T) DataSourceName {
	t.Helper()

	host, port := silentListener(t)

	return DataSourceName{
		User:         "u",
		Password:     "p",
		Host:         host,
		Port:         port,
		DatabaseName: "d",
		Charset:      "utf8mb4",
		Collation:    "utf8mb4_general_ci",
		Timeout:      "5s",
		ReadTimeout:  "5s",
		WriteTimeout: "5s",
	}
}

/*
 * buildDB 的單次連線必須聽 ctx。
 * ADR-0002 讓 consumer 的 drain 不設上限，前提是「所有對外呼叫皆 ctx-aware，取消即返回」；
 * ctx-unaware 的 db.Ping() 會把這一段的實際上限退回 DSN 的 GLOBAL_MARIADB_TIMEOUT，
 * 上游傳下來的 deadline（gRPC 的、handler 自訂的）在冷連線上一律失效。
 */
func TestBuildDBHonorsContextDeadline(t *testing.T) {
	dsn := silentDSN(t)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	db, err := dsn.buildDB(ctx)
	elapsed := time.Since(start)

	if err == nil {
		_ = db.Close()
		t.Fatal("buildDB() error = nil，期望 ctx 到期的錯誤")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("buildDB() error = %v，期望包住 context.DeadlineExceeded", err)
	}

	// DSN 的 timeout 是 5s，只要遠早於它返回，收手的就是 ctx 而不是 DSN
	if elapsed > time.Second {
		t.Errorf("buildDB() 耗時 %v，等到 DSN timeout 才返回；期望 ctx 到期即返回", elapsed)
	}
}

/*
 * 已經取消的 ctx 不該再開一次連線。
 * backoff 保證 operation 至少執行一次（ctx 檢查排在 operation 之後），所以「不再開連線」這件事
 * 只能由 buildDB 自己保證；少了它，呼叫端得在每兩段之間自己補 ctx 檢查
 */
func TestBuildDBReturnsImmediatelyOnCanceledContext(t *testing.T) {
	dsn := silentDSN(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	db, err := dsn.buildDB(ctx)
	elapsed := time.Since(start)

	if err == nil {
		_ = db.Close()
		t.Fatal("buildDB() error = nil，期望 context.Canceled")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("buildDB() error = %v，期望包住 context.Canceled", err)
	}

	if elapsed > 100*time.Millisecond {
		t.Errorf("buildDB() 耗時 %v，期望已取消的 ctx 讓它立刻返回", elapsed)
	}
}
