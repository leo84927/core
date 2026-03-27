package rabbitmq

import (
	"crypto/tls"
	"errors"
	"log"
	"net/url"

	"github.com/cenkalti/backoff/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

func permanentIfNeeded(err error) error {
	// 先判斷 err 是否為 nil，減少開銷（防呆）
	if err == nil {
		return nil
	}

	// URL 格式錯誤
	if urlErr := (*url.Error)(nil); errors.As(err, &urlErr) {
		log.Println("AMQP URL formatting error, url:", urlErr.URL, "err:", urlErr.Error())
		return backoff.Permanent(urlErr)
	}

	// TLS 憑證錯誤
	if tlsErr := (*tls.CertificateVerificationError)(nil); errors.As(err, &tlsErr) {
		return backoff.Permanent(tlsErr)
	}

	// AMQP 連線錯誤
	if amqpErr := (*amqp.Error)(nil); errors.As(err, &amqpErr) {
		switch amqpErr.Code {
		case
			// Connection & Channel 共用
			amqp.AccessRefused,   // 帳密錯誤、vhost 無權限
			amqp.NotAllowed,      // 連線數超過上限、vhost 不允許此操作
			amqp.InternalError,   // Broker 內部錯誤
			amqp.FrameError,      // Frame 格式錯誤
			amqp.SyntaxError,     // 語法錯誤
			amqp.CommandInvalid,  // 不合法的指令順序
			amqp.ChannelError,    // Channel 操作在非法狀態下執行
			amqp.UnexpectedFrame, // 非預期的 frame
			amqp.ResourceError,   // 資源設定衝突
			// Channel 專屬
			amqp.NotFound,           // queue/exchange 不存在
			amqp.PreconditionFailed, // queue 被 exclusive 鎖定
			amqp.ResourceLocked:     // 宣告參數與現有不符

			// 不可重試的錯誤，用 backoff.Permanent 包裝後後直接返回
			log.Println("AMQP can't retry error, code:", amqpErr.Code, "err:", amqpErr.Error())
			return backoff.Permanent(amqpErr)
		default:
			// 可重試的錯誤
			return amqpErr
		}
	}

	// 其他可重試的錯誤
	log.Println("Should retry error, err:", err.Error())
	return err
}
