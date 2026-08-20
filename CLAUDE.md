# core
封裝微服務之間共用的基礎建設

## Package 職責
| Package | 職責 |
|---|---|
| `config/` | 載入設定並轉為各服務的共用變數 |
| `logger/` | OTEL log + trace pipeline |
| `rabbitmq/` | RabbitMQ 完整生命週期 |
| `mariadb/` | MariaDB 讀寫分離連線管理 |
| `initialize/` | 微服務共用啟動流程封裝 |

## 共用設計模式
- 重試機制 + permanentIfNeeded
- singleflight 防併發連線
- Trace context 傳播
- Logger caller 來源位置

### 錯誤日誌入口
**core 內的錯誤日誌一律走 `logger.Error(ctx, msg, err, args...)`，不要直接呼叫 `slog.Error`。** Error 以外的錯誤等級不受此限。

### 錯誤源頭攜帶堆疊
eris 的堆疊只能在 `eris.New` / `eris.Wrap` 當下擷取，事後補救只會抓到記錄日誌那一行，對除錯沒有價值。

### Logger Close 模式
`Manager.Close()` 使用 `context.WithTimeout(context.Background(), 5s)` 而非外部傳入的 ctx，因為呼叫時 signal context 通常已 canceled，需要獨立的 context 讓 provider 有時間 flush
