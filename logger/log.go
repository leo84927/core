package logger

import (
	"context"
	"log/slog"

	"github.com/rotisserie/eris"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
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

// newSlogHandler 建立送往 OTEL 的 slog handler。
// WithSource 讓每則日誌帶上 code.file.path / code.function.name / code.line.number，
// 檔案路徑的裁剪靠建置時的 -trimpath（見根 CLAUDE.md 與 deploy.sh），
// 否則會把建置機的絕對路徑寫進雲端日誌。
func newSlogHandler(serviceName string, provider otellog.LoggerProvider) slog.Handler {
	return otelslog.NewHandler(
		serviceName,
		otelslog.WithLoggerProvider(provider),
		otelslog.WithSource(true),
	)
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
