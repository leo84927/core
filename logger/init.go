package logger

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	ServiceName string
	Host        string
	Port        string
}

type LogManager struct {
	Config   *Config
	exporter *otlploggrpc.Exporter
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
		otelslog.WithLoggerProvider(lm.provider), // 指定用哪個 Provider
	)

	slog.SetDefault(slog.New(handler))

	return nil
}

func (lm *LogManager) CloseLogger(ctx context.Context) {
	if lm.provider == nil {
		return
	}

	if err := lm.provider.Shutdown(ctx); err != nil {
		log.Printf("CloseLogger failed, err: %v\n", err)
		return
	}
}

func (lm *LogManager) setExporter(ctx context.Context) error {
	endPoint := fmt.Sprintf("%s:%s", lm.Config.Host, lm.Config.Port)
	exporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint(endPoint),
		otlploggrpc.WithDialOption(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		),
	)
	if err != nil {
		log.Printf("setExporter failed, err: %v\n", err)
		return err
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
		log.Printf("setResource failed, err: %v\n", err)
		return err
	}

	lm.resource = res
	return nil
}
