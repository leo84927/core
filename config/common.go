package config

import (
	goredis "github.com/redis/go-redis/v9"
)

var (
	RedisClient *goredis.Client
	EnvMap      map[string]string
	ServiceName string
)
