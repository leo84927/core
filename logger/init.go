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
	Endpoint    string // Grafana Endpoint
	AuthHeader  string // Grafana Token
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

/*
 * shutdownTimeout 是「每一個」exporter 各自的 flush 上限，不是兩者共用的總預算。
 * 共用一個 ctx 的話，log flush 慢就會把 trace 餓死 —— 而關機出問題時最需要看的正是 trace。
 */
const shutdownTimeout = 5 * time.Second

func (m *Manager) Close() {
	if m.logProvider != nil {
		shutdown(m.logProvider.Shutdown, "shutdown log provider failed")
	}

	if m.traceProvider != nil {
		shutdown(m.traceProvider.Shutdown, "shutdown trace provider failed")
	}
}

func shutdown(fn func(context.Context) error, msg string) {
	/* 
	 * 由於 close 時還會做 flush，如果使用傳進來的 ctx，會因為 ctx 早已先被取消，導致觸發 err
	 * 改成使用新的 ctx，並設定 timeout
	 */
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := fn(ctx); err != nil {
		fmt.Fprintln(os.Stderr, eris.Wrap(err, msg))
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
		return eris.Wrap(err, "new otel resource failed")
	}

	m.resource = res
	return nil
}
