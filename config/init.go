package config

import (
	"context"
	"log"
	"maps"
	"os"
	"time"

	cp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/consul"
	"github.com/leo84927/core/consul"
	"github.com/leo84927/core/redis"
)

func InitFromConsul(prefix string) {
	var err error

	// 建立 consul client，失敗時要直接 panic
	Client, err = consul.NewClient()
	if err != nil {
		log.Fatalf("new consul client failed, err: %v\n", err)
	}

	// 透過 consul 取得 global 環境變數，此時 EnvMap 為空可以直接指定
	if EnvMap, err = Client.List("GLOBAL"); err != nil {
		log.Fatalf("get env from consul failed, err: %v\n", err)
	}

	// 取得服務相關的環境變數，遍歷後指定給 EnvMap
	if serviceMap, err := Client.List(prefix); err != nil {
		log.Fatalf("get env from consul failed, err: %v\n", err)
	} else {
		maps.Copy(EnvMap, serviceMap)
	}

	// 指定 alloy
	AlloyHost = EnvMap[cp.GlobalEnvKey_GLOBAL_ALLOY_HOST.String()]
	AlloyPort = EnvMap[cp.GlobalEnvKey_GLOBAL_ALLOY_PORT.String()]

	// 指定時區
	TimeZone = EnvMap[cp.GlobalEnvKey_GLOBAL_TIMEZONE.String()]
	if Loc, err = time.LoadLocation(TimeZone); err != nil {
		log.Fatalf("load location failed, err: %v\n", err)
	}
}

func InitFromRedis(ctx context.Context, prefix string) {
	var err error

	cm := redis.NewConnectionManager(redis.Config{
		Host:           os.Getenv("REDIS_HOST"),
		Port:           os.Getenv("REDIS_PORT"),
		Password:       os.Getenv("REDIS_PASSWORD"),
		DB:             0,
		DialTimeout:    5 * time.Second,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		PoolSize:       10,
		MinIdleConns:   2,
		MaxRetries:     3,
		MaxElapsedTime: 30 * time.Second,
	})

	RedisClient, err = cm.Client(ctx)
	if err != nil {
		log.Fatalf("connect to redis failed, err: %v\n", err)
	}

	// 透過 redis 取得 global 環境變數，此時 EnvMap 為空可以直接指定
	if EnvMap, err = List(ctx, "GLOBAL:*"); err != nil {
		log.Fatalf("get env from redis failed, err: %v\n", err)
	}

	// 取得服務相關的環境變數，遍歷後指定給 EnvMap
	if serviceMap, err := List(ctx, prefix+":*"); err != nil {
		log.Fatalf("get env from redis failed, err: %v\n", err)
	} else {
		maps.Copy(EnvMap, serviceMap)
	}

	// 指定 alloy
	AlloyHost = EnvMap[cp.GlobalEnvKey_GLOBAL_ALLOY_HOST.String()]
	AlloyPort = EnvMap[cp.GlobalEnvKey_GLOBAL_ALLOY_PORT.String()]

	// 指定時區
	TimeZone = EnvMap[cp.GlobalEnvKey_GLOBAL_TIMEZONE.String()]
	if Loc, err = time.LoadLocation(TimeZone); err != nil {
		log.Fatalf("load location failed, err: %v\n", err)
	}
}

func List(ctx context.Context, pattern string) (map[string]string, error) {
	var keys []string

	// 先用 Scan 取得符合 pattern 的 keys，再透過 Iterator 遍歷 keys，最後把 keys 存到 keys slice 中
	iter := RedisClient.Scan(ctx, 0, pattern, 100).Iterator()
	if err := iter.Err(); err != nil {
		return nil, err
	}
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	// 透過 MGet 取值，再轉換成 map 回傳
	if len(keys) > 0 {
		vals, err := RedisClient.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, err
		}

		result := make(map[string]string, len(keys))
		for i, val := range vals {
			if val != nil {
				result[keys[i]] = val.(string)
			}
		}

		return result, nil
	}

	return nil, nil
}
