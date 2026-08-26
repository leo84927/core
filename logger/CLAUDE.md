# logger

## Logger Close 模式
`Manager.Close()` 使用 `context.WithTimeout(context.Background(), 5s)` 而非外部傳入的 ctx，因為呼叫時 signal context 通常已 canceled，需要獨立的 context 讓 provider 有時間 flush。  
log 與 trace 兩個 exporter **各自**一個 5s 的 ctx，共用會讓 log flush 餓死 trace。
