package logger

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/rotisserie/eris"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

type Config struct {
	ServiceName string
	Endpoint    string
	AuthHeader  string
}

type LogManager struct {
	Config   *Config
	exporter *otlploghttp.Exporter
	resource *resource.Resource
	provider *sdklog.LoggerProvider
}

func NewLogManager(config *Config) *LogManager {
	return &LogManager{
		Config: config,
	}
}

func (lm *LogManager) SetLogger(ctx context.Context) error {
	// 避免被重複呼叫時建立新的 provider，導致舊的 provider 沒有 shutdown
	if lm.provider != nil {
		return nil
	}

	if err := lm.setExporter(ctx); err != nil {
		return err
	}

	if err := lm.setResource(ctx); err != nil {
		return err
	}

	lm.provider = sdklog.NewLoggerProvider(
		sdklog.WithProcessor(
			sdklog.NewBatchProcessor(lm.exporter), // 批次送出，效能較好
		),
		sdklog.WithResource(lm.resource),
	)

	handler := otelslog.NewHandler(
		lm.Config.ServiceName,
		otelslog.WithLoggerProvider(lm.provider),
	)

	// slog.SetDefault 後，會改變 log.xxxxx 的行為，日誌會送到 handler 指定的目的地
	slog.SetDefault(slog.New(handler))

	return nil
}

func (lm *LogManager) CloseLogger(ctx context.Context) {
	if lm.provider == nil {
		return
	}

	if err := lm.provider.Shutdown(ctx); err != nil {
		fmt.Fprintln(os.Stderr, eris.Wrap(err, "shutdown logger provider failed"))
		return
	}
}

func (lm *LogManager) setExporter(ctx context.Context) error {
	fmt.Fprintln(os.Stdout, "Endpoint: "+lm.Config.Endpoint)
	fmt.Fprintln(os.Stdout, "AuthHeader: "+lm.Config.AuthHeader)
	exporter, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpoint(lm.Config.Endpoint),
		otlploghttp.WithURLPath("/otlp/v1/logs"),
		otlploghttp.WithHeaders(map[string]string{"Authorization": lm.Config.AuthHeader}),
	)
	if err != nil {
		return eris.Cause(err)
	}

	lm.exporter = exporter
	return nil
}

func (lm *LogManager) setResource(ctx context.Context) error {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(lm.Config.ServiceName),
			// semconv.ServiceVersion("1.0.0"), // 指定服務版本號，查看報錯時可以判斷屬於哪個版本
		),
	)
	if err != nil {
		return eris.Cause(err)
	}

	lm.resource = res
	return nil
}

func (lm *LogManager) Ping(ctx context.Context) error {
	if lm.provider == nil {
		return eris.New("provider not initialized")
	}

	slog.InfoContext(ctx, "ping",
		slog.String("service", lm.Config.ServiceName),
	)

	// 強制把 buffer 裡的 log 立刻送出（BatchProcessor 預設是延遲送）
	if err := lm.provider.ForceFlush(ctx); err != nil {
		return eris.Wrap(err, "ping failed")
	}

	return nil
}
