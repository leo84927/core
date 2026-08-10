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
rabbitmq、mariadb、redis 都使用 `cenkalti/backoff` 搭配各自的 `permanentIfNeeded()` 分類錯誤
（mariadb 與 redis 的版本會記錯誤日誌，因此簽章帶 `ctx`；rabbitmq 的不記日誌，維持只收 err）：
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

### 錯誤日誌入口
**core 內的錯誤日誌一律走 `logger.Error(ctx, msg, err, args...)`，不要直接呼叫 `slog.Error`。**
非錯誤等級（`slog.Info` / `slog.Debug`）不受此限，維持原樣。

錯誤以 OTEL 語意慣例的 `exception.stacktrace` 輸出成**單一多行字串**（來自 `eris.ToString(err, true)`）。
不用 `eris.ToJSON` 的巢狀 map，是因為那會在 Loki 端被展平成 `error_root_stack_0`、`error_root_stack_1`…
每個堆疊框一個欄位，既難讀又可能觸及 structured metadata 上限。

外部錯誤（driver、net 等）自身不帶 eris 堆疊，`eris.ToString` 對它們只會回傳「換行 + 訊息」。
`stacktrace()` 會 TrimSpace 掉前導換行，並用入口擷取到的 PC 補上呼叫端的框，格式沿用 eris 的
`func:file:line`。`rabbitmq` 的錯誤已改為自誕生起就帶堆疊（見下方「錯誤源頭攜帶堆疊」），
這層補償是 `mariadb` / `redis` / 其他仍未包裝的外部錯誤的備援，沒有它那些欄位會退化成單行。

`logger.Error` 內部自己抓 caller PC 並組 `slog.Record`，而非轉呼叫 `slog.ErrorContext` —— `slog` 的來源位置
是以寫死的跳層數擷取的，直接包一層會讓所有錯誤日誌的 `code.line.number` 指向 helper 內部。跳層數由
`callerSkip` 常數控制，並由 `TestErrorReportsCallerLineNotHelperInternals` 把關；**若在入口與呼叫端之間再加
一層包裝，該測試會失敗，記得同步調整 `callerSkip`**。

決策脈絡與被否決的替代方案見 monorepo 根目錄 `docs/adr/0001-error-logging-entry-point.md`。

### 錯誤源頭攜帶堆疊
**`rabbitmq` 內誕生的錯誤一律用 `eris.New`；外部錯誤（amqp091）在收到的第一時間用 `eris.Wrap` 包起來。**
eris 的堆疊只能在 `eris.New` / `eris.Wrap` 當下擷取，事後補救只會抓到記錄日誌那一行，對除錯沒有價值。
外部錯誤的「誕生」在 amqp091 內，我們能控制的最早一刻就是收到它的那一行，所以包裝點放在邊界：
`amqp.DialConfig`、`conn.Channel`、`ch.Qos`、`ch.Consume`、`ch.Confirm`、`PublishWithDeferredConfirm`、
`ExchangeDeclare` / `QueueDeclare` / `QueueBind`。

**不主動丟棄錯誤包裝鏈。** `rabbitmq` 的 `permanentIfNeeded` 只負責分類，`errors.As` 取出的 `*url.Error` /
`*tls.CertificateVerificationError` / `*amqp.Error` 僅用於判斷與記錄，**回傳一律是原本的 err**
（`backoff.Permanent(err)`）；換成底層錯誤會把包裝鏈連同堆疊一起丟掉。同理 `logger` 內不再用 `eris.Cause`，
改為 `eris.Wrap`。

`backoff.Retry` 的錯誤出口一律經過 `unwrapPermanent()`：`backoff` 只有在「還沒用完重試次數」時才會自己解開
`Permanent`，若不可重試的錯誤剛好落在最後一次嘗試，回傳的最外層會是 `*backoff.PermanentError`（非 eris 型別），
`eris` 只認得最外層，堆疊會整條看不到。`backoff` 的包裝屬於重試機制的內部細節，本來就不該外流給呼叫端。

**尚未套用的範圍：** `mariadb` / `redis` 的 `permanentIfNeeded` 仍回傳 `errors.As` 取出的底層錯誤，
且其外部錯誤沒有在邊界包裝，因此它們的 `exception.stacktrace` 仍靠 `logger.Error` 的 PC 補償。
`rabbitmq` 內只 log 不回傳的錯誤（`d.Ack` / `d.Nack` 失敗、`ConnectionManager.Close()`）也還是 `log.Println`，
不走 `logger.Error`，因此不會有 `exception.stacktrace`。這兩塊要另開票。

另外 `rabbitmq` 的 `log.Println` 在 `SetLogger` 之後會流進 slog（`slog.SetDefault` 會接管 `log` 套件的輸出），
所以那些訊息會以 INFO 進 Grafana；錯誤要有堆疊，靠的是錯誤本身往上傳到服務端以 `logger.Error` 記錄。

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
