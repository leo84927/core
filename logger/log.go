package logger

import (
	"context"
	"log/slog"

	"github.com/rotisserie/eris"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
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

	handler := otelslog.NewHandler(
		m.Config.ServiceName,
		otelslog.WithLoggerProvider(m.logProvider),
	)

	slog.SetDefault(slog.New(handler))

	return nil
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
