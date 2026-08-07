package redis

import (
	"context"

	"github.com/cenkalti/backoff/v5"
	"github.com/leo84927/core/logger"
	goredis "github.com/redis/go-redis/v9"
)

type ConnectionManager struct {
	config Config
	holder clientHolder
}

func NewConnectionManager(config Config) *ConnectionManager {
	return &ConnectionManager{
		config: config,
	}
}

func (cm *ConnectionManager) Client(ctx context.Context) (*goredis.Client, error) {
	if c := cm.holder.get(); c != nil {
		return c, nil
	}

	_, err, _ := cm.holder.sfg.Do("connect", func() (any, error) {
		return nil, cm.setClientWithRetry(ctx)
	})
	if err != nil {
		return nil, err
	}

	return cm.holder.get(), nil
}

// Close 沒有可用的請求 context（關閉時 signal context 通常已 canceled），用 Background
func (cm *ConnectionManager) Close() {
	if err := cm.holder.close(); err != nil {
		logger.Error(context.Background(), "failed to close redis", err)
	}
}

func (cm *ConnectionManager) setClientWithRetry(ctx context.Context) error {
	client, err := backoff.Retry(
		ctx,
		func() (*goredis.Client, error) {
			return cm.config.buildClient(ctx)
		},
		backoff.WithMaxElapsedTime(cm.config.MaxElapsedTime),
		backoff.WithMaxTries(cm.config.MaxRetries),
	)
	if err != nil {
		return err
	}

	cm.holder.set(client)
	return nil
}
