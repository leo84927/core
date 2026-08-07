package logger

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

const (
	// callerSkip 是 Error() 取得呼叫點時要跳過的框數：0=runtime.Callers、1=Error 自己，2 才是真正呼叫 Error 的那一行
	callerSkip = 2
	// stackDepth 是補堆疊時最多往上取幾層呼叫框
	stackDepth = 32
)

func (m *Manager) SetLogger(ctx context.Context) error {
	if m.logProvider != nil {
		return nil
	}

	if err := m.setLogExporter(ctx); err != nil {
		return err
	}

	if err := m.setResource(ctx); err != nil {
		return err
	}

	m.logProvider = sdklog.NewLoggerProvider(
		sdklog.WithProcessor(
			sdklog.NewBatchProcessor(m.logExporter),
		),
		sdklog.WithResource(m.resource),
	)

	slog.SetDefault(slog.New(newSlogHandler(m.Config.ServiceName, m.logProvider)))

	return nil
}

/*
 * newSlogHandler 建立送往 OTEL 的 slog handler
 * WithSource 讓每則日誌帶上 code.file.path / code.function.name / code.line.number
 * 檔案路徑的裁剪靠建置時的 -trimpath（見根 CLAUDE.md 與 deploy.sh），否則會把建置機的絕對路徑寫進雲端日誌
 */
func newSlogHandler(serviceName string, provider otellog.LoggerProvider) slog.Handler {
	return otelslog.NewHandler(
		serviceName,
		otelslog.WithLoggerProvider(provider),
		otelslog.WithSource(true),
	)
}

/*
 * Error 是 core 的錯誤日誌入口，所有錯誤日誌都應該走這裡
 * 錯誤以 OTEL 語意慣例的 exception.stacktrace 輸出成『單一多行字串』
 */
func Error(ctx context.Context, msg string, err error, args ...any) {
	logger := slog.Default()
	if !logger.Enabled(ctx, slog.LevelError) {
		return
	}

	var pcs [stackDepth]uintptr
	n := runtime.Callers(callerSkip, pcs[:])

	record := slog.NewRecord(time.Now(), slog.LevelError, msg, pcs[0])
	if err != nil {
		record.AddAttrs(
			slog.String(
				string(semconv.ExceptionStacktraceKey),
				stacktrace(err, pcs[:n]),
			),
		)
	}
	record.Add(args...)

	_ = logger.Handler().Handle(ctx, record)
}

/*
 * stacktrace 把錯誤格式化成單一多行字串
 * eris 的堆疊只能在 eris.New / eris.Wrap 當下擷取，外部錯誤（driver、net 等）自身沒有堆疊，eris.ToString 對它們只會回傳「換行 + 訊息」
 */
func stacktrace(err error, pcs []uintptr) string {
	// eris.ToString 對外部錯誤會在開頭多一個換行，直接送出去會讓欄位以空行起頭
	formatted := strings.TrimSpace(eris.ToString(err, true))

	if len(eris.StackFrames(err)) > 0 || len(pcs) == 0 {
		return formatted
	}

	var builder strings.Builder
	builder.WriteString(formatted)

	frames := runtime.CallersFrames(pcs)
	for {
		frame, more := frames.Next()
		fmt.Fprintf(&builder, "\n\t%s:%s:%d", frame.Function, frame.File, frame.Line)
		if !more {
			break
		}
	}

	return builder.String()
}

func (m *Manager) setLogExporter(ctx context.Context) error {
	opts := []otlploghttp.Option{
		otlploghttp.WithEndpoint(m.Config.Endpoint),
		otlploghttp.WithURLPath("/otlp/v1/logs"),
	}

	if m.Config.AuthHeader != "" {
		opts = append(opts, otlploghttp.WithHeaders(map[string]string{
			"Authorization": m.Config.AuthHeader,
		}))
	}

	exporter, err := otlploghttp.New(ctx, opts...)
	if err != nil {
		return eris.Cause(err)
	}

	m.logExporter = exporter
	return nil
}
