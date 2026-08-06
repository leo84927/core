# core
封裝微服務之間共用的基礎建設，每個服務透過 `go get github.com/leo84927/core` 引用

## Package 職責

| Package | 職責 |
|---|---|
| `config/` | 從 Upstash Redis 載入設定（`GLOBAL:*` + `<PREFIX>:*`），解析為 rabbitmq / mariadb / logger / timezone 等模組的 config |
| `consul/` | （已棄用）Consul client 封裝 |
| `logger/` | OTEL log + trace pipeline：透過 otlploghttp / otlptracehttp 將 slog 日誌與 span 送到 Grafana Cloud，並為每則日誌帶上 caller 來源位置 |
| `rabbitmq/` | RabbitMQ 完整生命週期：連線管理、topology 宣告、producer（publish confirm）、consumer（ack/nack） |
| `mariadb/` | MariaDB 讀寫分離連線管理：主庫寫、從庫讀，lazy connect |
| `initialize/` | 微服務共用啟動流程封裝（開發中）：`New()` → 組裝 → `Run()` → `Close()` |

## 設定載入流程

```
config.InitFromRedis(ctx, prefix)
  ├── redis.NewConnectionManager()   // 需要 REDIS_HOST / REDIS_PORT / REDIS_PASSWORD 環境變數
  ├── List(ctx, "GLOBAL:*")          // 載入全域設定
  ├── List(ctx, prefix+":*")         // 載入服務專屬設定（如 "TELEGRAM:*"）
  └── 設定 GrafanaEndpoint/GrafanaAuthHeader、TimeZone/Loc
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

### Trace context 傳播（rabbitmq）
producer 透過 `amqpHeaderCarrier` 將 traceparent 注入 AMQP headers（`otel.Inject`），consumer 從 headers 萃取後建立子 span（`otel.Extract`），實現跨服務的分散式追蹤

### Logger caller 來源位置
`newSlogHandler()` 對 `otelslog` 開啟 `WithSource(true)`，因此**所有**使用 core 的服務，每則日誌都會多三個欄位：

| 欄位 | 內容 |
|---|---|
| `code.file.path` | 呼叫點的檔案路徑 |
| `code.function.name` | 呼叫點的函式名稱 |
| `code.line.number` | 呼叫點的行號 |

`code.file.path` 取自 binary 內記錄的編譯期路徑，**建置必須帶 `-trimpath`**，否則會把建置機的絕對路徑送進 Grafana Cloud。裁剪後 core 自身為 `github.com/leo84927/core/logger/log.go`（非 main module 一律用 module path），服務為 `telegram/handle/webhook.go`。詳見 monorepo 根目錄 `CLAUDE.md` 的「日誌來源路徑」。

`newSlogHandler()` 把 provider 當參數收，是為了讓測試能塞進記憶體 provider，不必真的連到 Grafana。

### Logger Close 模式
`Manager.Close()` 使用 `context.WithTimeout(context.Background(), 5s)` 而非外部傳入的 ctx，因為呼叫時 signal context 通常已 canceled，需要獨立的 context 讓 provider 有時間 flush

## 指令

```sh
# 引用
go get github.com/leo84927/core

# 更新
go get -u github.com/leo84927/core

# 測試
go test ./rabbitmq/ -race -v -count=1
# 含覆蓋率
go test ./rabbitmq/ -race -v -count=1 -coverprofile=coverage.out
# 好讀版
gotestsum --format testname -- ./rabbitmq/ -race -v -count=1

# logger：必須帶 -trimpath，否則路徑裁剪的那條測試只會 Skip
go test -trimpath ./logger/ -race -v -count=1
```
