package consul

import (
	"context"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/rotisserie/eris"
)

func (c *Client) SendHeartbeat(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// 程式關閉時，主動將狀態標記為 Critical
			_ = c.agent.UpdateTTL(c.checkId, "service shutting down", api.HealthCritical)
			return nil // 正常關閉不回傳 error，避免觸發 errgroup 的錯誤處理
		case <-ticker.C:
			// 定期發送心跳
			if err := c.agent.UpdateTTL(c.checkId, "ok", api.HealthPassing); err != nil {
				return eris.Cause(err) // 回傳 error，讓 errgroup 知道
			}
		}
	}
}
