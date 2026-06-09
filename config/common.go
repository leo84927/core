package config

import (
	"github.com/leo84927/core/consul"
	goredis "github.com/redis/go-redis/v9"
)

var (
	Client      *consul.Client
	RedisClient *goredis.Client
	EnvMap      map[string]string
	ServiceName string
)
