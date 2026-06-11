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
	"github.com/rotisserie/eris"
	"golang.org/x/sync/errgroup"
)

type App struct {
	LogManager *logger.Manager
	RabbitmqCM *rabbitmq.ConnectionManager

	connReady chan struct{}
	Consumer  *rabbitmq.Consumer

	Workers []func(ctx context.Context) error
}

func New(ctx context.Context) (*App, error) {
	var app = &App{
		connReady: make(chan struct{}),
	}

	app.LogManager = logger.NewManager(&logger.Config{
		ServiceName: config.ServiceName,
		Endpoint:    config.GrafanaEndpoint,
		AuthHeader:  config.GrafanaAuthHeader,
	})
	// 輸出 log 到 grafana
	err := app.LogManager.SetLogger(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "set logger failed, err: %v\n", err)
		return nil, err
	}
	// 輸出 trace 到 grafana
	err = app.LogManager.SetTracer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "set tracer failed, err: %v\n", err)
		return nil, err
	}

	// 初始化 rabbitmq
	app.RabbitmqCM = rabbitmq.NewConnectionManager(config.GetRabbitMQConfig().Config)

	return app, nil
}

func (app *App) Close(ctx context.Context) {
	app.RabbitmqCM.Close()

	app.LogManager.Close()

	if r := recover(); r != nil {
		err := fmt.Errorf("recovered: %v\n%s", r, debug.Stack())
		fmt.Fprintln(os.Stderr, err)
	}
}

func (app *App) Run(ctx context.Context) {
	group, groupCtx := errgroup.WithContext(ctx)

	graceful(group, func() error {
		// 這裏預設 consumer 的 goroutine 透過 Workers 傳遞，所以當 Workers 不為 0 代表需要建立 consumer
		if len(app.Workers) != 0 {
			app.Consumer = app.RabbitmqCM.NewConsumer(config.GetRabbitMQConfig().ServiceQueue.Name, "", 5, 20*time.Second)

			// 建立連線＆拓樸
			if err := app.RabbitmqCM.InitTopology(groupCtx, config.GetRabbitMQConfig().Topology); err != nil {
				return err
			}
		}

		// 建立 ready 檔案用來做 health check
		if err := os.WriteFile("/tmp/ready", []byte("ok"), 0644); err != nil {
			return err
		}

		// 不論是 producer 或 consumer 都要關閉
		close(app.connReady)

		// 開始監控連線狀態並自動重試
		slog.Info("rabbitmq connection and topology ready")
		return app.RabbitmqCM.WatchConnAndRetry(groupCtx)
	})

	graceful(group, func() error {
		select {
		case <-app.connReady:
		case <-groupCtx.Done():
			return groupCtx.Err()
		}

		// 每個 worker 各自用 graceful 啟動
		for _, w := range app.Workers {
			graceful(group, func() error { return w(groupCtx) })
		}

		return nil
	})

	// 等待所有 goroutine 結束
	if err := group.Wait(); err != nil {
		slog.Error(
			"shutdown with err",
			"error", eris.ToJSON(err, true),
		)
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
