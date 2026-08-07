package mariadb

import (
	"context"
	"crypto/tls"
	"errors"
	"net/url"

	"github.com/cenkalti/backoff/v5"
	"github.com/go-sql-driver/mysql"
	"github.com/leo84927/core/logger"
)

func permanentIfNeeded(ctx context.Context, err error) error {
	// 先判斷 err 是否為 nil，減少開銷（防呆）
	if err == nil {
		return nil
	}

	// DSN 格式錯誤
	if urlErr := (*url.Error)(nil); errors.As(err, &urlErr) {
		logger.Error(ctx, "dsn formatting error", err, "dsn", urlErr.URL)
		return backoff.Permanent(urlErr)
	}

	// TLS 憑證錯誤
	if tlsErr := (*tls.CertificateVerificationError)(nil); errors.As(err, &tlsErr) {
		logger.Error(ctx, "mariadb tls certificate verification error", err)
		return backoff.Permanent(tlsErr)
	}

	// MySQL 連線錯誤
	if mysqlErr := (*mysql.MySQLError)(nil); errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case
			// 不可重試的錯誤，用 backoff.Permanent 包裝後直接返回
			1044, // Access denied for user to database
			1045, // Access denied for user（帳密錯誤）
			1049, // Unknown database
			1130, // Host not allowed to connect
			1131, // Anonymous connections are not allowed
			1133, // User not found
			1227, // Access denied（insufficient privileges）
			1251, // Client does not support authentication protocol
			1275: // Server is running in safe mode

			logger.Error(ctx, "mysql can't retry error", mysqlErr)
			return backoff.Permanent(mysqlErr)
		default:
			// 可重試的錯誤（e.g. 1040 too many connections、網路瞬斷）
			return mysqlErr
		}
	}

	// 其他可重試的錯誤（e.g. connection refused、i/o timeout）
	logger.Error(ctx, "should retry error", err)
	return err
}
