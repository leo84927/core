package redis

import (
	"crypto/tls"
	"errors"
	"log/slog"
	"net/url"

	"github.com/cenkalti/backoff/v5"
	goredis "github.com/redis/go-redis/v9"
	"github.com/rotisserie/eris"
)

func permanentIfNeeded(err error) error {
	if err == nil {
		return nil
	}

	// URL 格式錯誤
	if urlErr := (*url.Error)(nil); errors.As(err, &urlErr) {
		slog.Error(
			"redis url formatting error",
			"url", urlErr.URL,
		)
		return backoff.Permanent(urlErr)
	}

	// TLS 憑證錯誤
	if tlsErr := (*tls.CertificateVerificationError)(nil); errors.As(err, &tlsErr) {
		slog.Error(
			"redis tls certificate verification error",
			"error", eris.ToJSON(err, true),
		)
		return backoff.Permanent(tlsErr)
	}

	// Redis 錯誤
	if redisErr := (goredis.Error)(nil); errors.As(err, &redisErr) {
		msg := redisErr.Error()

		// NOAUTH / WRONGPASS：密碼錯誤或未認證
		if goredis.HasErrorPrefix(err, "NOAUTH") || goredis.HasErrorPrefix(err, "WRONGPASS") {
			slog.Error(
				"redis auth error",
				"error", eris.ToJSON(err, true),
			)
			return backoff.Permanent(err)
		}

		// ERR DB index is out of range
		if goredis.HasErrorPrefix(err, "ERR DB index") {
			slog.Error(
				"redis db index out of range",
				"error", eris.ToJSON(err, true),
			)
			return backoff.Permanent(err)
		}

		// 其他 Redis 錯誤視為可重試
		slog.Error(
			"redis should retry error",
			"error", msg,
		)
		return err
	}

	// 其他可重試的錯誤（connection refused、i/o timeout 等）
	slog.Error(
		"should retry error",
		"error", eris.ToJSON(err, true),
	)
	return err
}
