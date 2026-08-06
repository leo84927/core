package logger

import (
	"context"
	"log/slog"
	"runtime"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// recordingProcessor 把 log record 留在記憶體，讓測試不需要真的連到 Grafana
type recordingProcessor struct {
	records []sdklog.Record
}

func (p *recordingProcessor) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }

func (p *recordingProcessor) OnEmit(_ context.Context, record *sdklog.Record) error {
	p.records = append(p.records, record.Clone())
	return nil
}

func (p *recordingProcessor) Shutdown(context.Context) error   { return nil }
func (p *recordingProcessor) ForceFlush(context.Context) error { return nil }

// newTestLogger 組出一個寫進記憶體的 logger，與 SetLogger 走同一個 handler
func newTestLogger() (*slog.Logger, *recordingProcessor) {
	processor := &recordingProcessor{}
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(processor))
	return slog.New(newSlogHandler("test-service", provider)), processor
}

func attrs(record sdklog.Record) map[string]log.Value {
	result := make(map[string]log.Value, record.AttributesLen())
	record.WalkAttributes(func(kv log.KeyValue) bool {
		result[kv.Key] = kv.Value
		return true
	})
	return result
}

// 日誌必須帶上呼叫點的檔案、函式名稱、行號，且指向呼叫 slog 的那一行，而非 bridge 內部
func TestNewSlogHandlerAttachesCallerSource(t *testing.T) {
	logger, processor := newTestLogger()

	_, _, line, _ := runtime.Caller(0)
	logger.Info("hello") // 必須緊接在上一行之後，行號為 line + 1

	if len(processor.records) != 1 {
		t.Fatalf("record 數量 = %d, 期望 1", len(processor.records))
	}

	got := attrs(processor.records[0])

	filePath, ok := got["code.file.path"]
	if !ok {
		t.Fatalf("缺少 code.file.path，實際 attributes = %v", got)
	}
	if !strings.HasSuffix(filePath.AsString(), "core/logger/log_test.go") {
		t.Errorf("code.file.path = %q, 期望結尾為 core/logger/log_test.go", filePath.AsString())
	}

	functionName, ok := got["code.function.name"]
	if !ok {
		t.Fatalf("缺少 code.function.name，實際 attributes = %v", got)
	}
	if !strings.HasSuffix(functionName.AsString(), "TestNewSlogHandlerAttachesCallerSource") {
		t.Errorf("code.function.name = %q, 期望結尾為 TestNewSlogHandlerAttachesCallerSource", functionName.AsString())
	}

	lineNumber, ok := got["code.line.number"]
	if !ok {
		t.Fatalf("缺少 code.line.number，實際 attributes = %v", got)
	}
	if lineNumber.AsInt64() != int64(line+1) {
		t.Errorf("code.line.number = %d, 期望 %d（呼叫 slog 的那一行）", lineNumber.AsInt64(), line+1)
	}
}

// -trimpath 後檔案路徑不得含建置機的絕對路徑。
// 注意：此測試只在建置帶了 -trimpath 時才有效，需用 `go test -trimpath ./logger/` 執行，
// 一般 `go test` 會走 Skip（見 core/CLAUDE.md 的測試指令）。
func TestCallerFilePathIsNotAbsoluteWhenTrimmed(t *testing.T) {
	logger, processor := newTestLogger()

	logger.Info("hello")

	filePath := attrs(processor.records[0])["code.file.path"].AsString()

	if strings.HasPrefix(filePath, "/") {
		t.Skipf("此次建置未帶 -trimpath，path = %q", filePath)
	}
	if !strings.HasPrefix(filePath, "github.com/leo84927/core/logger/") {
		t.Errorf("code.file.path = %q, 期望以模組路徑 github.com/leo84927/core/logger/ 開頭", filePath)
	}
}
