package rabbitmq

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v5"
)

type Exchange struct {
	Name string
	Kind string // direct, fanout, topic, headers
}

type Queue struct {
	Name string
	Keys []string
}

type Topology struct {
	Exchange       Exchange      // 一個 Exchange
	Queues         []Queue       // 對應多個 Queue
	MaxElpasedTime time.Duration // 總重試時間上限
	MaxRetries     uint          // 最大重試次數上限
}

// InitTopology：宣告 topology，包含重試邏輯，外部初始化時必須另外呼叫，WatchConnAndRetry 在最一開始不負責初始化 topology
func (cm *ConnectionManager) InitTopology(ctx context.Context, topology Topology) error {
	operation := func() (struct{}, error) {
		conn, err := cm.connect(ctx)
		if err != nil {
			return struct{}{}, permanentIfNeeded(err)
		}

		ch, err := conn.Channel()
		if err != nil {
			return struct{}{}, permanentIfNeeded(err)
		}
		defer func() {
			_ = ch.Close()
		}()

		if err := cm.declareTopology(ch, topology); err != nil {
			return struct{}{}, permanentIfNeeded(err)
		}

		return struct{}{}, nil
	}

	_, err := backoff.Retry(
		context.Background(),
		operation,
		backoff.WithMaxElapsedTime(topology.MaxElpasedTime),
		backoff.WithMaxTries(topology.MaxRetries),
	)
	if err != nil {
		return err
	}

	// 寫入宣告成功的 topology，重連時 replay
	cm.topology.Store(&topology)

	return nil
}

func (cm *ConnectionManager) declareTopology(ch AMQPChannel, topology Topology) error {
	// 一個 Exchange -> 多個 Queue
	err := ch.ExchangeDeclare(
		topology.Exchange.Name, // name
		topology.Exchange.Kind, // direct, fanout, topic, headers
		true,                   // durable
		false,                  // auto delete
		false,                  // internal
		false,                  // no wait
		nil,                    // args
	)
	if err != nil {
		return err
	}

	// 有幾個 Queue 就 declare 幾次
	for _, queue := range topology.Queues {
		_, err := ch.QueueDeclare(
			queue.Name, // name
			true,       // durable
			false,      // auto delete
			false,      // exclusive
			false,      // no wait
			nil,        // args
		)
		if err != nil {
			return err
		}

		// 綁定 Exchange 和當前的 Queue
		for _, key := range queue.Keys {
			// 有幾個規則就綁定幾次
			err = ch.QueueBind(queue.Name, key, topology.Exchange.Name, false, nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
