package database

import (
	"sync"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/singleflight"
)

// dbHolder 將單一 DB 的狀態集中管理，避免在 ConnectionManager 層出現 **sqlx.DB
type dbHolder struct {
	db  *sqlx.DB
	mu  sync.RWMutex
	sfg singleflight.Group
}

func (h *dbHolder) get() *sqlx.DB {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.db
}

func (h *dbHolder) set(db *sqlx.DB) {
	h.mu.Lock()
	h.db = db
	h.mu.Unlock()
}

func (h *dbHolder) close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.db == nil {
		return nil
	}

	err := h.db.Close()
	h.db = nil

	return err
}
