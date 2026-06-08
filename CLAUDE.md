# core
封裝微服務之間共用的基礎建設，每個服務透過 `go get github.com/leo84927/core` 引用

## Package 職責

| Package | 職責 |
|---|---|
| `config/` | 從 Consul KV 載入設定，解析為 rabbitmq / mariadb / logger / timezone 等模組的 config |
| `consul/` | Consul client 封裝：KV 讀取、TTL health check 心跳 |
| `logger/` | OTEL log pipeline：透過 otlploggrpc 將 slog 日誌送到 Grafana Alloy |
| `rabbitmq/` | RabbitMQ 完整生命週期：連線管理、topology 宣告、producer（publish confirm）、consumer（ack/nack） |
| `mariadb/` | MariaDB 讀寫分離連線管理：主庫寫、從庫讀，lazy connect |
| `initialize/` | 微服務共用啟動流程封裝（開發中）：`New()` → 組裝 → `Run()` → `Close()` |

## 設定載入流程

```
config.InitFromConsul(prefix)
  ├── consul.NewClient()           // 需要 CONSUL_HTTP_ADDR 環境變數
  ├── Client.List("GLOBAL")        // 載入全域設定
  ├── Client.List(prefix)          // 載入服務專屬設定（如 "EXCHANGE_RATE"）
  └── 設定 AlloyHost/Port、TimeZone/Loc
```

各服務在 `init()` 中依需求接續呼叫：
- `config.LoadBasicRabbitMQ()` — 所有使用 RabbitMQ 的服務
- `config.LoadBasicTopology()` — 只需 exchange 的 producer
- `config.LoadCompleteTopology(queue)` — 需要完整 queue binding 的 consumer
- `config.LoadMariaDbConfig()` — 需要存取資料庫的服務

## 共用設計模式

### 重試 + permanentIfNeeded
rabbitmq 和 mariadb 都使用 `cenkalti/backoff` 搭配各自的 `permanentIfNeeded()` 分類錯誤：
- 不可重試（帳密錯誤、格式錯誤等）→ `backoff.Permanent` 立即停止
- 可重試（網路瞬斷、連線數滿等）→ 交給 backoff 重試

### singleflight 防併發連線
rabbitmq `ConnectionManager.connect()` 和 mariadb `dbHolder` 都用 `singleflight` 確保多個 goroutine 同時要求連線時，只有一個真正建立

### Interface 抽象（rabbitmq）
`interface.go` 定義 `AMQPConnection`、`AMQPChannel` 等介面，包裝 `amqp091-go` 的具體型別，測試時可替換為 mock

## 指令

```sh
# 引用
go get github.com/leo84927/core

# 更新
go get -u github.com/leo84927/core

# 測試
go test ./...
```
