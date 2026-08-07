package logger

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"testing"

	"github.com/rotisserie/eris"
	"go.opentelemetry.io/otel/log"
)

// useTestLogger 把 slog 預設 logger 換成寫進記憶體的版本，測完還原。
// Error() 走的是 slog.Default()，所以測試必須從這個入口驗證，才會連 caller 跳層數一起測到。
func useTestLogger(t *testing.T) *recordingProcessor {
	t.Helper()

	logger, processor := newTestLogger()
	original := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(original) })

	return processor
}

// 堆疊必須是單一個 exception.stacktrace 字串欄位，不能是會在 Loki 端被展平的巢狀結構
func TestErrorAttachesStacktraceAsSingleStringField(t *testing.T) {
	processor := useTestLogger(t)

	Error(context.Background(), "connect failed", eris.New("dial tcp: connection refused"))

	if len(processor.records) != 1 {
		t.Fatalf("record 數量 = %d, 期望 1", len(processor.records))
	}

	got := attrs(processor.records[0])

	stacktrace, ok := got["exception.stacktrace"]
	if !ok {
		t.Fatalf("缺少 exception.stacktrace，實際 attributes = %v", got)
	}
	if stacktrace.Kind() != log.KindString {
		t.Errorf("exception.stacktrace 的 Kind = %v, 期望 KindString（巢狀結構會被 Loki 展平）", stacktrace.Kind())
	}
	if !strings.Contains(stacktrace.AsString(), "\n") {
		t.Errorf("exception.stacktrace = %q, 期望是多行堆疊", stacktrace.AsString())
	}
	if !strings.Contains(stacktrace.AsString(), "dial tcp: connection refused") {
		t.Errorf("exception.stacktrace = %q, 期望含原始錯誤訊息", stacktrace.AsString())
	}
}

// 不得再出現 error_root_stack_0 之類會被展平的欄位：所有 attribute 都必須是純量
func TestErrorEmitsNoNestedAttributes(t *testing.T) {
	processor := useTestLogger(t)

	Error(context.Background(), "connect failed", eris.New("boom"))

	processor.records[0].WalkAttributes(func(kv log.KeyValue) bool {
		switch kv.Value.Kind() {
		case log.KindMap, log.KindSlice:
			t.Errorf("attribute %q 的 Kind = %v，巢狀結構會被 Loki 展平成 %s_0、%s_1…", kv.Key, kv.Value.Kind(), kv.Key, kv.Key)
		}
		return true
	})

	if _, ok := attrs(processor.records[0])["error"]; ok {
		t.Error("仍存在舊的 error 欄位，應已改為 exception.stacktrace")
	}
}

// 行號必須指向呼叫 Error() 的那一行，而非 helper 內部（ticket 1 的 caller 資訊不能被 helper 吃掉）
func TestErrorReportsCallerLineNotHelperInternals(t *testing.T) {
	processor := useTestLogger(t)

	_, file, line, _ := runtime.Caller(0)
	Error(context.Background(), "connect failed", eris.New("boom")) // 必須是 line + 1

	got := attrs(processor.records[0])

	lineNumber, ok := got["code.line.number"]
	if !ok {
		t.Fatalf("缺少 code.line.number，實際 attributes = %v", got)
	}
	if lineNumber.AsInt64() != int64(line+1) {
		t.Errorf("code.line.number = %d, 期望 %d（呼叫 Error 的那一行，而非 helper 內部）", lineNumber.AsInt64(), line+1)
	}

	filePath, ok := got["code.file.path"]
	if !ok {
		t.Fatalf("缺少 code.file.path，實際 attributes = %v", got)
	}
	if !strings.HasSuffix(filePath.AsString(), "error_test.go") {
		t.Errorf("code.file.path = %q, 期望指向呼叫端 %s，而非 log.go", filePath.AsString(), file)
	}

	functionName, ok := got["code.function.name"]
	if !ok {
		t.Fatalf("缺少 code.function.name，實際 attributes = %v", got)
	}
	if !strings.HasSuffix(functionName.AsString(), "TestErrorReportsCallerLineNotHelperInternals") {
		t.Errorf("code.function.name = %q, 期望是呼叫端的測試函式", functionName.AsString())
	}
}

// 訊息與額外的 key-value 要照常帶上
func TestErrorKeepsMessageAndExtraAttributes(t *testing.T) {
	processor := useTestLogger(t)

	Error(context.Background(), "connect failed", eris.New("boom"), "dsn", "user@tcp(host)/db")

	record := processor.records[0]
	if body := record.Body().AsString(); body != "connect failed" {
		t.Errorf("body = %q, 期望 %q", body, "connect failed")
	}
	if severity := record.SeverityText(); severity != slog.LevelError.String() {
		t.Errorf("severity = %q, 期望 %q", severity, slog.LevelError.String())
	}
	if dsn := attrs(record)["dsn"]; dsn.AsString() != "user@tcp(host)/db" {
		t.Errorf("dsn = %q, 期望附加的 key-value 會被保留", dsn.AsString())
	}
}

// 外部錯誤（driver、net 等自身不帶 eris 堆疊）也必須有多行堆疊，否則 core 現有的呼叫點
// 幾乎全是外部錯誤，exception.stacktrace 會只剩一行訊息
func TestErrorHandlesErrorWithoutErisStack(t *testing.T) {
	processor := useTestLogger(t)

	Error(context.Background(), "connect failed", errors.New("Error 1045: Access denied"))

	stacktrace := attrs(processor.records[0])["exception.stacktrace"]
	if stacktrace.Kind() != log.KindString {
		t.Fatalf("exception.stacktrace 的 Kind = %v, 期望 KindString", stacktrace.Kind())
	}

	value := stacktrace.AsString()
	if !strings.Contains(value, "Error 1045: Access denied") {
		t.Errorf("exception.stacktrace = %q, 期望含外部錯誤的訊息", value)
	}
	if !strings.Contains(value, "TestErrorHandlesErrorWithoutErisStack") {
		t.Errorf("exception.stacktrace = %q, 期望用呼叫端的框補上堆疊", value)
	}
	if !strings.Contains(value, "\n") {
		t.Errorf("exception.stacktrace = %q, 期望是多行", value)
	}
}

// eris.ToString 對外部錯誤會回傳開頭帶換行的字串，不能直接送出去
func TestErrorStacktraceHasNoLeadingBlankLine(t *testing.T) {
	processor := useTestLogger(t)

	Error(context.Background(), "connect failed", errors.New("Error 1045: Access denied"))

	value := attrs(processor.records[0])["exception.stacktrace"].AsString()
	if value != strings.TrimLeft(value, " \t\n") {
		t.Errorf("exception.stacktrace = %q, 開頭不應有空白或換行", value)
	}
}

// err 為 nil 時不該 panic，也不該憑空生出堆疊
func TestErrorWithNilError(t *testing.T) {
	processor := useTestLogger(t)

	Error(context.Background(), "connect failed", nil)

	if len(processor.records) != 1 {
		t.Fatalf("record 數量 = %d, 期望 1", len(processor.records))
	}
	if _, ok := attrs(processor.records[0])["exception.stacktrace"]; ok {
		t.Error("err 為 nil 時不應帶 exception.stacktrace")
	}
}
