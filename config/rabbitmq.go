package config

import (
	"log"
	"strconv"
	"time"

	env "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/env"
	"github.com/leo84927/core/rabbitmq"
)

var rabbitmqCfg RabbitMQ

type RabbitMQ struct {
	Config       *rabbitmq.Config
	Topology     rabbitmq.Topology
	ServiceQueue rabbitmq.Queue
}

func LoadBasicRabbitMQ() {
	connMaxRetries, err := strconv.Atoi(EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_CONN_MAX_RETRIES.String()])
	if err != nil {
		log.Printf("transfer RABBITMQ_CONN_MAX_RETRIES failed, invalid value: %v, error: %v\n", EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_CONN_MAX_RETRIES.String()], err)
		connMaxRetries = 5
	}
	connMaxElapsed, err := time.ParseDuration(EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_CONN_MAX_ELAPSED_TIME.String()])
	if err != nil {
		log.Printf("transfer RABBITMQ_CONN_MAX_ELAPSED_TIME failed, invalid value: %v, error: %v\n", EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_CONN_MAX_ELAPSED_TIME.String()], err)
		connMaxElapsed = 20 * time.Second
	}

	rabbitmqCfg.Config = &rabbitmq.Config{
		ServiceName:    ServiceName,
		User:           EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_USER.String()],
		Password:       EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_PASSWORD.String()],
		Host:           EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_HOST.String()],
		Port:           EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_PORT.String()],
		Vhost:          EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_VHOST.String()],
		MaxRetries:     uint(connMaxRetries),
		MaxElapsedTime: connMaxElapsed,
	}
}

// 若只是 producer，只需基本的 topology
func LoadBasicTopology() {
	rabbitmqCfg.Topology.Exchange.Name = EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_JOB_EXCHANGE.String()]
}

// 若是 consumer，就需要完整的 topology
func LoadCompleteTopology(serviceQueue rabbitmq.Queue) {
	topologyMaxRetries, err := strconv.Atoi(EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_TOPOLOGY_MAX_RETRIES.String()])
	if err != nil {
		log.Printf("transfer RABBITMQ_TOPOLOGY_MAX_RETRIES failed, invalid value: %v, error: %v\n", EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_TOPOLOGY_MAX_RETRIES.String()], err)
		topologyMaxRetries = 3
	}
	topologyMaxElapsed, err := time.ParseDuration(EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME.String()])
	if err != nil {
		log.Printf("transfer RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME failed, invalid value: %v, error: %v\n", EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME.String()], err)
		topologyMaxElapsed = 20 * time.Second
	}

	rabbitmqCfg.Topology = rabbitmq.Topology{
		Exchange: rabbitmq.Exchange{
			Name: EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_JOB_EXCHANGE.String()],
			Kind: EnvMap[env.GlobalEnvKey_GLOBAL_RABBITMQ_EXCHANGE_KIND.String()],
		},
		Queues: []rabbitmq.Queue{
			serviceQueue,
		},
		MaxRetries:     uint(topologyMaxRetries),
		MaxElpasedTime: topologyMaxElapsed,
	}

	rabbitmqCfg.ServiceQueue = serviceQueue
}

func GetRabbitMQConfig() RabbitMQ {
	return rabbitmqCfg
}
