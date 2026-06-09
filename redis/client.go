package redis

import (
	"sync"

	goredis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type clientHolder struct {
	client *goredis.Client
	mu     sync.RWMutex
	sfg    singleflight.Group
}

func (h *clientHolder) get() *goredis.Client {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.client
}

func (h *clientHolder) set(client *goredis.Client) {
	h.mu.Lock()
	h.client = client
	h.mu.Unlock()
}

func (h *clientHolder) close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.client == nil {
		return nil
	}

	err := h.client.Close()
	h.client = nil

	return err
}
