# rabbitmq

## 訊息確認契約
work-then-Ack、per-message goroutine、prefetch 寫死 4、否認一律 `Nack(false, false)`，決策與理由見 `docs/adr/0002-message-acknowledgement-contract.md`（monorepo 根目錄）。  
`core` 刻意不匯出任何 sentinel error 或錯誤型別 —— 不做錯誤分類。

## 三組重試預算
語意不同，刻意分成三組，不共用欄位：

| 預算 | 值 | 來源 | 用在 |
|---|---|---|---|
| 連線重連 | 設定鍵 | `GLOBAL:RABBITMQ_CONN_MAX_*` | `WatchConnAndRetry` |
| 發布重試 | `{3, 5s}` | 寫死在 `NewConnectionManager` | `PublishWithRetry` |
| 消費重連 | `{5, 20s}` | 寫死在 `NewConnectionManager` | `WaitForConsume` |
