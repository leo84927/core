package rabbitmq

type Exchange struct {
	Name string
	Kind string // direct, fanout, topic, headers
}

type Queue struct {
	Name string
	Keys []string
}

type Topology struct {
	Exchange Exchange // 一個 Exchange
	Queues   []Queue  // 對應多個 Queue
}

func (cm *ConnectionManager) InitTopology(topology Topology) {
	conn, err := cm.GetConn()
	if err != nil {
		panic(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	// 一個 Exchange -> 多個 Queue
	err = ch.ExchangeDeclare(
		topology.Exchange.Name, // name
		topology.Exchange.Kind, // direct, fanout, topic, headers
		true,                   // durable
		false,                  // auto delete
		false,                  // internal
		false,                  // no wait
		nil,                    // args
	)
	if err != nil {
		panic(err)
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
			panic(err)
		}

		// 綁定 Exchange 和當前的 Queue
		for _, key := range queue.Keys {
			// 有幾個規則就綁定幾次
			err = ch.QueueBind(queue.Name, key, topology.Exchange.Name, false, nil)
			if err != nil {
				panic(err)
			}
		}
	}
}
