# Connection - conn.go

### Complete

- lazy connect
- retry
- watch & auto-reconnect
- topology replay



# Topology - topology.go

### Complete

- retry
- persistence for replay



# Producer - producer.go

### Complete

- retry
- confirm



# Consumer - consumer.go

### Complete

- retry
- QoS
- Ack/Nack
- requeue



# Incomplete

### Poison Message (HIGH)
目前 Nack(requeue=true) 時，會導致無限循環。需要一起實作：

- retry 次數限制（x-death header 或消息自帶 count）
- DLX / DLQ 宣告（topology 初始化時）
- 超過上限後 Nack(requeue=false) 路由到 DLQ

### Consumer Graceful Shutdown (MEDIUM)
目前 ctx 取消會直接丟棄已推送但未處理的消息，broker 需要重新投遞，增加重複消費的機率。需要一起實作：

- ch.Cancel() 通知 broker 停止推送
- 排空 msgs channel 裡剩餘的消息
- sync.WaitGroup 等待 in-flight 消息處理完畢

### Message 持久化與基本屬性設定 (MEDIUM)
目前 producer 沒有設定 DeliveryMode，broker 重啟後消息會遺失。這幾個通常一起決定：

- DeliveryMode: 2（消息持久化）
- Queue durable 已有，但需確認 Exchange 也是 durable（目前是，但沒有對外開放設定）
- Message TTL（避免積壓過久的消息佔用資源）
- Queue 最大長度（避免無限積壓）

### Concurrent Consumer (low)
目前單一 goroutine 處理消息，吞吐量受限。需要一起實作：

- 多個 goroutine 同時處理消息
- sync.WaitGroup 控制生命週期（與 Graceful Shutdown 強相關，建議同時實作）
- QoS prefetch 數量需對應調整

### Channel Pool (low)
目前每次 publish 都開新 channel，高頻 publish 時開關 channel 的開銷很大。需要一起實作：

- Channel 的建立、借用、歸還
- Channel 健康檢查（channel 異常時從 pool 移除）
- Pool 大小上限控制

### Multi-vhost / Connection Pool (lowest)
目前只支援單一 connection，有兩種情境會需要：

- 不同 vhost 需要不同 connection
- 單一 connection 的 channel 數量有上限（RabbitMQ 預設 2047），高併發時需要多條 connection

### Quorum Queue / 消息優先級 (lowest)
屬於進階功能，只有特定情境才需要，且依賴 RabbitMQ cluster 環境，優先級最低：

- Quorum queue：cluster 環境下的資料安全
- 消息優先級：需要 queue 和 producer 同時配合設定
