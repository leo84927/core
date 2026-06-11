package logger

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rotisserie/eris"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

type Config struct {
	ServiceName string
	Endpoint    string
	AuthHeader  string
}

type Manager struct {
	Config        *Config
	resource      *resource.Resource
	logExporter   *otlploghttp.Exporter
	logProvider   *sdklog.LoggerProvider
	traceExporter sdktrace.SpanExporter
	traceProvider *sdktrace.TracerProvider
}

func NewManager(config *Config) *Manager {
	return &Manager{
		Config: config,
	}
}

func (m *Manager) Close() {
	// 由於 close 時還會做 flush，如果使用傳進來的 ctx，會因為 ctx 早已先被取消，導致觸發 err；改成使用新的 ctx，並設定 timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if m.logProvider != nil {
		if err := m.logProvider.Shutdown(ctx); err != nil {
			fmt.Fprintln(os.Stderr, eris.Wrap(err, "shutdown log provider failed"))
		}
	}

	if m.traceProvider != nil {
		if err := m.traceProvider.Shutdown(ctx); err != nil {
			fmt.Fprintln(os.Stderr, eris.Wrap(err, "shutdown trace provider failed"))
		}
	}
}

func (m *Manager) setResource(ctx context.Context) error {
	if m.resource != nil {
		return nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(m.Config.ServiceName),
		),
	)
	if err != nil {
		return eris.Cause(err)
	}

	m.resource = res
	return nil
}
