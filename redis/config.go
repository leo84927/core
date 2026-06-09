package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Config struct {
	Host     string
	Port     string
	Password string
	DB       int

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	PoolSize     int
	MinIdleConns int

	MaxRetries     uint
	MaxElapsedTime time.Duration
}

func (c *Config) buildClient(ctx context.Context) (*goredis.Client, error) {
	client := goredis.NewClient(&goredis.Options{
		Addr:         fmt.Sprintf("%s:%s", c.Host, c.Port),
		Password:     c.Password,
		DB:           c.DB,
		DialTimeout:  c.DialTimeout,
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
		PoolSize:     c.PoolSize,
		MinIdleConns: c.MinIdleConns,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, permanentIfNeeded(err)
	}

	return client, nil
}
