package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	ServiceName    string
	User           string
	Password       string
	Host           string
	Port           string
	Vhost          string
	MaxElpasedTime time.Duration
	MaxRetries     uint
}

type ConnectionManager struct {
	Config *Config
	mutex  sync.RWMutex
	conn   *amqp.Connection
}

func NewConnectionManager(config *Config) *ConnectionManager {
	return &ConnectionManager{
		Config: config,
	}
}

/**
 * 這個寫法代表只能設定一個連線
 * 如果之後有多個 vhost 再來重構
 */
func (cm *ConnectionManager) SetConnWithRetry() error {
	conn, err := backoff.Retry(
		context.Background(),
		cm.Config.buildConnection,
		backoff.WithMaxElapsedTime(cm.Config.MaxElpasedTime), // 總重試時間上限
		backoff.WithMaxTries(cm.Config.MaxRetries),           // 最大重試次數上限
	)
	if err != nil {
		log.Println("SetConnWithRetry failed, err:", err.Error())
		return err
	}

	cm.setConn(conn)
	return nil
}

func (cm *ConnectionManager) WatchConnAndRetry(ctx context.Context) error {
	closeCh := cm.GetConn().NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping WatchConnAndRetry")
			return nil
		case err, ok := <-closeCh:
			// 正常關閉
			if !ok || err == nil {
				log.Println("connection closed")
				return nil
			}

			// 異常關閉，開始重試連線
			log.Println("connection closed, err:", err.Error())
			if err := cm.SetConnWithRetry(); err != nil {
				log.Println("failed to reconnect, err:", err.Error())
				return err
			}

			closeCh = cm.GetConn().NotifyClose(make(chan *amqp.Error, 1))
		}
	}
}

func (cm *ConnectionManager) GetConn() *amqp.Connection {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	if cm.conn == nil {
		// TODO: 這裡會造成 deadlock，必需修正
		cm.SetConnWithRetry()
	}
	return cm.conn
}

func (cm *ConnectionManager) Close() {
	err := cm.GetConn().Close()
	if err != nil {
		panic(err)
	}
}

func (cm *ConnectionManager) setConn(conn *amqp.Connection) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	cm.conn = conn
}

func (config *Config) buildUrl() string {
	url := fmt.Sprintf(
		"amqp://%s:%s@%s:%s/%s",
		config.User,
		config.Password,
		config.Host,
		config.Port,
		config.Vhost,
	)
	log.Println("amqp url:", url)
	return url
}

func (config *Config) buildConnection() (*amqp.Connection, error) {
	return amqp.DialConfig(
		config.buildUrl(),
		amqp.Config{
			Heartbeat: 10 * time.Second, // 由 golang 每 n 秒確認一次 broker 是否存活
			Locale:    "en_US",
			Properties: amqp.Table{
				"connection_name": config.ServiceName, // 該次連線是由哪個服務所建立的
			},
			Dial: func(network, addr string) (net.Conn, error) {
				d := net.Dialer{
					Timeout:   5 * time.Second,  // TCP 交握的超時時間
					KeepAlive: 30 * time.Second, // 由 OS 每 n 秒確認一次連線是否存活
				}
				return d.Dial(network, addr)
			},
		},
	)
}
