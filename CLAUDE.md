# core
封裝微服務之間共用的基礎建設

## Package 職責
| Package | 職責 |
|---|---|
| `config/` | 從 Redis 載入設定，回傳純值 `Settings` |
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

### 設定載入回傳純值
`config.Load(ctx, Spec)` 只回傳值，副作用與其關閉義務留在 `initialize.New` / `app.Close` 同一層，理由見 `docs/adr/0003-settings-as-pure-values.md`（monorepo 根目錄）。

型別轉換失敗一律報錯，沒有預設值 fallback；「鍵不存在」報錯、「鍵存在但值為空」放行。

Redis 鍵名 -> `GLOBAL:MARIADB:CONN_MAX_ELAPSED_TIME`  
enum 鍵名 -> `GLOBAL_MARIADB_CONN_MAX_ELAPSED_TIME`  
所以只能從 Redis 鍵名轉換至 enum 鍵名，`:` 一律換成 `_`。

### 錯誤源頭攜帶堆疊
eris 的堆疊只能在 `eris.New` / `eris.Wrap` 當下擷取，事後補救只會抓到記錄日誌那一行，對除錯沒有價值。
