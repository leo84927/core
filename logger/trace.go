package logger

import (
	"context"

	"github.com/rotisserie/eris"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func (m *Manager) SetTracer(ctx context.Context) error {
	if m.traceProvider != nil {
		return nil
	}

	if err := m.setTraceExporter(ctx); err != nil {
		return err
	}

	if err := m.setResource(ctx); err != nil {
		return err
	}

	m.traceProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(m.traceExporter),
		sdktrace.WithResource(m.resource),
	)

	otel.SetTracerProvider(m.traceProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return nil
}

func (m *Manager) setTraceExporter(ctx context.Context) error {
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(m.Config.Endpoint),
		otlptracehttp.WithURLPath("/otlp/v1/traces"),
	}

	if m.Config.AuthHeader != "" {
		opts = append(opts, otlptracehttp.WithHeaders(map[string]string{
			"Authorization": m.Config.AuthHeader,
		}))
	}

	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return eris.Cause(err)
	}

	m.traceExporter = exporter
	return nil
}
