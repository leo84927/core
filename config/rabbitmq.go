package config

import (
	env "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/env"
	"github.com/leo84927/core/rabbitmq"
)

type RabbitMQ struct {
	Config   rabbitmq.Config
	Topology rabbitmq.Topology
	Queue    *rabbitmq.Queue // nil = 只當 producer
}

func (r *reader) rabbitMQ(serviceName string, keys *QueueKeys) RabbitMQ {
	cfg := RabbitMQ{
		Config: rabbitmq.Config{
			ServiceName:    serviceName,
			User:           r.str(env.GlobalEnvKey_GLOBAL_RABBITMQ_USER),
			Password:       r.str(env.GlobalEnvKey_GLOBAL_RABBITMQ_PASSWORD),
			Host:           r.str(env.GlobalEnvKey_GLOBAL_RABBITMQ_HOST),
			Port:           r.str(env.GlobalEnvKey_GLOBAL_RABBITMQ_PORT),
			Vhost:          r.str(env.GlobalEnvKey_GLOBAL_RABBITMQ_VHOST),
			MaxRetries:     r.uint(env.GlobalEnvKey_GLOBAL_RABBITMQ_CONN_MAX_RETRIES),
			MaxElapsedTime: r.duration(env.GlobalEnvKey_GLOBAL_RABBITMQ_CONN_MAX_ELAPSED_TIME),
		},
	}
	cfg.Topology.Exchange.Name = r.str(env.GlobalEnvKey_GLOBAL_RABBITMQ_JOB_EXCHANGE)

	// 只是 producer 就只需要基本的 topology（宣告 exchange），不需要 queue binding 那幾個鍵
	if keys == nil {
		return cfg
	}

	queue := rabbitmq.Queue{
		Name: r.str(keys.NameKey),
		Keys: []string{r.str(keys.RoutingKey)},
	}

	cfg.Queue = &queue
	cfg.Topology.Exchange.Kind = r.str(env.GlobalEnvKey_GLOBAL_RABBITMQ_EXCHANGE_KIND)
	cfg.Topology.Queues = []rabbitmq.Queue{queue}
	cfg.Topology.MaxRetries = r.uint(env.GlobalEnvKey_GLOBAL_RABBITMQ_TOPOLOGY_MAX_RETRIES)
	cfg.Topology.MaxElapsedTime = r.duration(env.GlobalEnvKey_GLOBAL_RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME)

	return cfg
}
