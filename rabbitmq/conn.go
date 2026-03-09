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
	MaxElpasedTime time.Duration // 總重試時間上限
	MaxRetries     uint          // 最大重試次數上限
}

type ConnectionManager struct {
	Config *Config
	mutex  sync.RWMutex
	conn   *amqp.Connection
	once   sync.Once
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
		backoff.WithMaxElapsedTime(cm.Config.MaxElpasedTime),
		backoff.WithMaxTries(cm.Config.MaxRetries),
	)
	if err != nil {
		log.Println("SetConnWithRetry failed, err:", err.Error())
		return err
	}

	cm.setConn(conn)
	return nil
}

func (cm *ConnectionManager) WatchConnAndRetry(ctx context.Context) error {
	conn, err := cm.GetConn()
	if err != nil {
		log.Println("failed to get connection, err:", err.Error())
		return err
	}
	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping WatchConnAndRetry")
			return nil
		case amqpErr, ok := <-closeCh:
			// 正常關閉
			if !ok || amqpErr == nil {
				log.Println("connection closed")
				return nil
			}

			// 異常關閉，開始重試連線
			log.Println("connection closed, err:", amqpErr.Error())
			if setConnErr := cm.SetConnWithRetry(); setConnErr != nil {
				log.Println("failed to reconnect, err:", setConnErr.Error())
				return setConnErr
			}

			conn, err = cm.GetConn()
			if err != nil {
				log.Println("failed to get connection, err:", err.Error())
				return err
			}
			closeCh = conn.NotifyClose(make(chan *amqp.Error, 1))
		}
	}
}

func (cm *ConnectionManager) GetConn() (*amqp.Connection, error) {
	var err error
	cm.once.Do(func() {
		// 透過 sync.Once 實現 lazy initialization，之後呼叫 GetConn 就不會再執行 SetConnWithRetry，但呼叫者要持續執行 WatchConnAndRetry 以確保連線可用
		err = cm.SetConnWithRetry()
	})
	if err != nil {
		return nil, err
	}

	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.conn, nil
}

func (cm *ConnectionManager) Close() {
	conn, err := cm.GetConn()
	if err != nil {
		panic(err)
	}

	err = conn.Close()
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
	conn, err := amqp.DialConfig(
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
	if err != nil {
		return nil, permanentIfNeeded(err)
	}

	return conn, nil
}
