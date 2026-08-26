# rabbitmq

## 訊息確認契約
work-then-Ack、per-message goroutine、prefetch 寫死 4、否認一律 `Nack(false, false)`，決策與理由見 `docs/adr/0002-message-acknowledgement-contract.md`（monorepo 根目錄）。  
`core` 刻意不匯出任何 sentinel error 或錯誤型別 —— 不做錯誤分類。
