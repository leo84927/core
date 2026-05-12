# Connection - conn.go

### Complete
- lazy connect
- retry

### Incomplete

- Metrics / Observability（最優先）
效能分析

- Health Check
有了 Metrics 之後，才能知道 Health Check 的失敗率是否值得警戒。
本身實作簡單，但要注意不要用寫庫做 Ping，避免產生不必要的寫入壓力。

- Query-level retry（最後，且要謹慎）
寫入操作不能無條件 retry，核心問題是冪等性（Idempotency）。

