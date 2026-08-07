package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"net/url"

	"github.com/cenkalti/backoff/v5"
	"github.com/leo84927/core/logger"
	goredis "github.com/redis/go-redis/v9"
)

func permanentIfNeeded(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	// URL 格式錯誤
	if urlErr := (*url.Error)(nil); errors.As(err, &urlErr) {
		logger.Error(ctx, "redis url formatting error", err, "url", urlErr.URL)
		return backoff.Permanent(urlErr)
	}

	// TLS 憑證錯誤
	if tlsErr := (*tls.CertificateVerificationError)(nil); errors.As(err, &tlsErr) {
		logger.Error(ctx, "redis tls certificate verification error", err)
		return backoff.Permanent(tlsErr)
	}

	// Redis 錯誤
	if redisErr := (goredis.Error)(nil); errors.As(err, &redisErr) {
		// NOAUTH / WRONGPASS：密碼錯誤或未認證
		if goredis.HasErrorPrefix(err, "NOAUTH") || goredis.HasErrorPrefix(err, "WRONGPASS") {
			logger.Error(ctx, "redis auth error", err)
			return backoff.Permanent(err)
		}

		// ERR DB index is out of range
		if goredis.HasErrorPrefix(err, "ERR DB index") {
			logger.Error(ctx, "redis db index out of range", err)
			return backoff.Permanent(err)
		}

		// 其他 Redis 錯誤視為可重試
		logger.Error(ctx, "redis should retry error", err)
		return err
	}

	// 其他可重試的錯誤（connection refused、i/o timeout 等）
	logger.Error(ctx, "should retry error", err)
	return err
}
