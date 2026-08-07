package initialize

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"time"

	"github.com/leo84927/core/config"
	"github.com/leo84927/core/logger"
	"github.com/leo84927/core/rabbitmq"
	"golang.org/x/sync/errgroup"
)

type App struct {
	logManager *logger.Manager
	MQWorker   MQWorker
	HttpWorker HttpWorker
}

func New(ctx context.Context, app *App) (*App, error) {
	app.logManager = logger.NewManager(&logger.Config{
		ServiceName: config.ServiceName,
		Endpoint:    config.GrafanaEndpoint,
		AuthHeader:  config.GrafanaAuthHeader,
	})
	// 輸出 log 到 grafana
	err := app.logManager.SetLogger(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "set logger failed, err: %v\n", err)
		return nil, err
	}
	// 輸出 trace 到 grafana
	err = app.logManager.SetTracer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "set tracer failed, err: %v\n", err)
		return nil, err
	}

	// 初始化 rabbitmq
	if cfg := config.GetRabbitMQConfig(); cfg.Config != nil {
		app.MQWorker.connReady = make(chan struct{})
		app.MQWorker.RabbitmqCM = rabbitmq.NewConnectionManager(cfg.Config)

		if app.MQWorker.MsgHandler != nil && cfg.ServiceQueue.Name != "" {
			app.MQWorker.Consumer = app.MQWorker.RabbitmqCM.NewConsumer(cfg.ServiceQueue.Name, "", 5, 20*time.Second)
		}
	}

	return app, nil
}

func (app *App) Close(ctx context.Context) {
	if app.MQWorker.RabbitmqCM != nil {
		app.MQWorker.RabbitmqCM.Close()
	}

	app.logManager.Close()

	if r := recover(); r != nil {
		err := fmt.Errorf("recovered: %v\n%s", r, debug.Stack())
		fmt.Fprintln(os.Stderr, err)
	}
}

func (app *App) Run(ctx context.Context) {
	group, groupCtx := errgroup.WithContext(ctx)

	if app.MQWorker.RabbitmqCM != nil {
		graceful(group, func() error { return app.MQWorker.ConnectionExecution(groupCtx) })
	}
	if app.MQWorker.MsgHandler != nil {
		graceful(group, func() error { return app.MQWorker.ConsumerExecution(groupCtx) })
	}

	if app.HttpWorker.WebhookServer != nil {
		graceful(group, func() error { return app.HttpWorker.WebhookServer(groupCtx) })
	}

	if app.HttpWorker.GrpcServer != nil {
		graceful(group, func() error { return app.HttpWorker.GrpcServer(groupCtx) })
	}

	// 等待所有 goroutine 結束
	if err := group.Wait(); err != nil {
		logger.Error(ctx, "shutdown with err", err)
		return
	}

	slog.Info("normal shutdown")
}

// 包裝 errgroup，就可以不用每個 goroutine 都宣告 defer recover
func graceful(g *errgroup.Group, fn func() error) {
	g.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("recovered: %v\n%s", r, debug.Stack())
				fmt.Fprintln(os.Stderr, err)
			}
		}()

		return fn()
	})
}
