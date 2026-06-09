package redis

import (
	"context"
	"log/slog"

	"github.com/cenkalti/backoff/v5"
	goredis "github.com/redis/go-redis/v9"
	"github.com/rotisserie/eris"
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

func (cm *ConnectionManager) Close() {
	if err := cm.holder.close(); err != nil {
		slog.Error(
			"failed to close redis",
			"error", eris.ToJSON(err, true),
		)
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
