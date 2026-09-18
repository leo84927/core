package initialize

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"

	"github.com/leo84927/core/v2/config"
	"github.com/leo84927/core/v2/logger"
	"github.com/leo84927/core/v2/rabbitmq"
	"golang.org/x/sync/errgroup"
)

type App struct {
	logManager *logger.Manager
	MQWorker   MQWorker
	HttpWorker HttpWorker
}

/*
 * 副作用（logger／tracer／AMQP 連線）都在這一層開，並由 app.Close 配對關閉。
 * config.Load 保持純值回傳，詳見 docs/adr/0003。
 */
func New(ctx context.Context, settings config.Settings, app *App) (*App, error) {
	app.logManager = logger.NewManager(&settings.Logger)
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

	/*
	 * 初始化 rabbitmq：無條件執行。四個服務全部用 MQ，Load 保證 RabbitMQ.Config 已填好，
	 * 所以不再有「設定漏載入 → 跳過整段 → connReady 是 nil channel → consumer 永久阻塞」的狀態。
	 */
	app.MQWorker.connReady = make(chan struct{})
	app.MQWorker.topology = settings.RabbitMQ.Topology
	app.MQWorker.RabbitmqCM = rabbitmq.NewConnectionManager(&settings.RabbitMQ.Config)

	/*
	 * Spec.Queue 與 MsgHandler 必須同時有或同時沒有 —— 兩者是同一件事（「這個服務是 consumer」）
	 * 的兩半，各自宣告在不同的地方，對不起來的兩個方向都是靜默的：
	 *
	 * 有 handler 沒 queue → Run 在 nil Consumer 上 panic，被 graceful recover 接住後服務結束，
	 *   而錯誤訊息只是一個 nil pointer 的堆疊，看不出真正的原因。
	 * 有 queue 沒 handler → ConnectionExecution 跳過 InitTopology，queue 與 binding 從未建立，
	 *   producer 發來的訊息在 exchange 上找不到 binding 而被丟棄，兩端都不會有任何日誌。
	 *
	 * 所以在這裡就失敗，而不是讓其中一半安靜地不生效。
	 */
	if hasHandler, hasQueue := app.MQWorker.MsgHandler != nil, settings.RabbitMQ.Queue != nil; hasHandler != hasQueue {
		err := fmt.Errorf("consumer setup is half-declared: message handler registered = %t, Spec.Queue declared = %t", hasHandler, hasQueue)
		fmt.Fprintln(os.Stderr, err)
		return nil, err
	}

	if app.MQWorker.MsgHandler != nil {
		app.MQWorker.Consumer = app.MQWorker.RabbitmqCM.NewConsumer(settings.RabbitMQ.Queue.Name, "")
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

func (app *App) Run(ctx context.Context) error {
	group, groupCtx := errgroup.WithContext(ctx)

	// New 無條件建立連線管理器，所以這一段不再有條件
	graceful(group, func() error { return app.MQWorker.ConnectionExecution(groupCtx) })

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
		return err
	}

	slog.Info("normal shutdown")
	return nil
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
