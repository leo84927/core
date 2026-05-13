package mariadb

import (
	"context"
	"log/slog"

	"github.com/cenkalti/backoff/v5"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rotisserie/eris"
)

type ConnectionManager struct {
	config *Config
	write  dbHolder
	read   dbHolder
}

func NewConnectionManager(config *Config) *ConnectionManager {
	return &ConnectionManager{
		config: config,
	}
}

// Write：取得寫庫，供 INSERT / UPDATE / DELETE 使用
func (cm *ConnectionManager) Write(ctx context.Context) (*sqlx.DB, error) {
	return cm.lazyConnect(ctx, &cm.write, cm.config.WriteDB)
}

// Read：取得讀庫，供 SELECT 使用
func (cm *ConnectionManager) Read(ctx context.Context) (*sqlx.DB, error) {
	return cm.lazyConnect(ctx, &cm.read, cm.config.ReadDB)
}

func (cm *ConnectionManager) Close() {
	if err := cm.write.close(); err != nil {
		slog.Error(
			"failed to close writeDB",
			"error", eris.ToJSON(err, true),
		)
	}

	if err := cm.read.close(); err != nil {
		slog.Error(
			"failed to close readDB",
			"error", eris.ToJSON(err, true),
		)
	}
}

func (cm *ConnectionManager) lazyConnect(ctx context.Context, holder *dbHolder, dsn DataSourceName) (*sqlx.DB, error) {
	if db := holder.get(); db != nil {
		return db, nil
	}

	_, err, _ := holder.sfg.Do("connect", func() (any, error) {
		return nil, cm.setDBWithRetry(ctx, holder, dsn)
	})
	if err != nil {
		return nil, err
	}

	return holder.get(), nil
}

// setDBWithRetry：根據傳入的 holder 決定將初始化後的實例指定給讀庫或寫庫
func (cm *ConnectionManager) setDBWithRetry(ctx context.Context, holder *dbHolder, dsn DataSourceName) error {
	db, err := backoff.Retry(
		ctx,
		func() (*sqlx.DB, error) {
			return dsn.buildDB()
		},
		backoff.WithMaxElapsedTime(cm.config.MaxElapsedTime),
		backoff.WithMaxTries(cm.config.MaxRetries),
	)
	if err != nil {
		return err
	}

	holder.set(db)
	return nil
}
