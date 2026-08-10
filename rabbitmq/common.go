package rabbitmq

import (
	"crypto/tls"
	"errors"
	"log"
	"net/url"

	"github.com/cenkalti/backoff/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

/*
 * permanentIfNeeded 只負責分類，errors.As 取出的底層錯誤僅用於判斷與記錄
 * 回傳一律用原本的 err，不能換成底層錯誤，否則會連同 eris 堆疊一起把包裝鏈丟掉
 */
func permanentIfNeeded(err error) error {
	// 先判斷 err 是否為 nil，減少開銷（防呆）
	if err == nil {
		return nil
	}

	// URL 格式錯誤
	if urlErr := (*url.Error)(nil); errors.As(err, &urlErr) {
		log.Println("AMQP URL formatting error, url:", urlErr.URL, "err:", urlErr.Error())
		return backoff.Permanent(err)
	}

	// TLS 憑證錯誤
	if tlsErr := (*tls.CertificateVerificationError)(nil); errors.As(err, &tlsErr) {
		return backoff.Permanent(err)
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
			return backoff.Permanent(err)
		default:
			// 可重試的錯誤
			return err
		}
	}

	// 其他可重試的錯誤
	log.Println("Should retry error, err:", err.Error())
	return err
}

/*
 * unwrapPermanent 解開 backoff 的 Permanent 包裝，所有 backoff.Retry 的錯誤出口都要經過這裡
 * backoff.Retry 只有在「還沒用完重試次數」時才會自己解開；不可重試的錯誤若剛好落在最後一次嘗試，
 * 回傳的最外層會是 *backoff.PermanentError。eris 只認得最外層，屆時 exception.stacktrace 會整條看不到堆疊
 */
func unwrapPermanent(err error) error {
	if permanent := (*backoff.PermanentError)(nil); errors.As(err, &permanent) {
		return permanent.Unwrap()
	}

	return err
}
