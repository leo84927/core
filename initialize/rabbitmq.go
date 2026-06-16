package initialize

import (
	"context"
	"log/slog"

	"github.com/leo84927/core/config"
	"github.com/leo84927/core/rabbitmq"
)

type MQWorker struct {
	RabbitmqCM *rabbitmq.ConnectionManager
	Consumer   *rabbitmq.Consumer
	connReady  chan struct{}
	MsgHandler rabbitmq.MsgHandler
}

func (worker *MQWorker) ConnectionExecution(ctx context.Context) error {
	if worker.MsgHandler != nil {
		// 建立連線＆拓樸
		if err := worker.RabbitmqCM.InitTopology(ctx, config.GetRabbitMQConfig().Topology); err != nil {
			return err
		}
	}

	// 通知 consumer 拓樸已建立（就算沒有 consumer 也要 close）
	close(worker.connReady)

	slog.Info("rabbitmq connection and topology ready")
	return worker.RabbitmqCM.WatchConnAndRetry(ctx)
}

func (worker *MQWorker) ConsumerExecution(ctx context.Context) error {
	select {
	case <-worker.connReady:
	case <-ctx.Done():
		return ctx.Err()
	}

	slog.Info("consumer start")
	return worker.Consumer.WaitForConsume(ctx, worker.MsgHandler)
}
