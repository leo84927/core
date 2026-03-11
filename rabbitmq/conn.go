package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
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
	MaxRetries     uint          // 最大重試次數上限
	MaxElpasedTime time.Duration // 總重試時間上限
}

type ConnectionManager struct {
	Config   *Config
	mutex    sync.RWMutex
	conn     AMQPConnection
	topology atomic.Pointer[Topology]

	// 允許在測試時替換建立連線的函式
	dialFunc func() (AMQPConnection, error)
}

func NewConnectionManager(config *Config) *ConnectionManager {
	cm := &ConnectionManager{
		Config: config,
	}

	// 預設使用真實連線
	cm.dialFunc = func() (AMQPConnection, error) {
		conn, err := config.buildConnection()
		if err != nil {
			return nil, err
		}

		return &amqpConnection{conn}, nil
	}

	return cm
}

func (cm *ConnectionManager) WatchConnAndRetry(ctx context.Context) error {
	// 檢查連線是否存在並取得連線
	conn, err := cm.connect()
	if err != nil {
		log.Println("connect failed, err:", err.Error())
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

			// 異常關閉，重新取得連線，並更新 closeCh
			log.Println("connection closed, err:", amqpErr.Error())
			conn, err = cm.connect()
			if err != nil {
				log.Println("failed to connect, err:", err.Error())
				return err
			}
			closeCh = conn.NotifyClose(make(chan *amqp.Error, 1))

			// 重新宣告 topology
			topology := cm.topology.Load()
			if topology != nil {
				err = cm.InitTopology(*topology)
				if err != nil {
					log.Println("failed to declare topology, err:", err.Error())
					return err
				}
			}
		}
	}
}

func (cm *ConnectionManager) Close() {
	conn, err := cm.getConn()
	if err != nil {
		log.Println("Close failed, err:", err.Error())
		return
	}

	err = conn.Close()
	if err != nil {
		log.Println("Close failed, err:", err.Error())
		return
	}
}

// connect：明確建立連線（含 lazy 語意）
func (cm *ConnectionManager) connect() (AMQPConnection, error) {
	// 先檢查連線是否存在
	cm.mutex.RLock()
	if cm.conn != nil && !cm.conn.IsClosed() {
		// 已有連線，直接返回
		cm.mutex.RUnlock()
		return cm.conn, nil
	}
	cm.mutex.RUnlock()

	// 連線不存在
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// 雙重檢查，確保在獲取鎖期間沒有其他 goroutine 已經建立了連線
	if cm.conn != nil && !cm.conn.IsClosed() {
		return cm.conn, nil
	}

	// 建立新連線
	err := cm.setConnWithRetry()
	if err != nil {
		log.Println("Failed to establish connection, err:", err.Error())
		return nil, err
	}

	return cm.conn, nil
}

// getConn：純粹取得連線，不負責重連
func (cm *ConnectionManager) getConn() (AMQPConnection, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	if cm.conn == nil || cm.conn.IsClosed() {
		return nil, fmt.Errorf("no active connection")
	}
	return cm.conn, nil
}

/**
 * setConnWithRetry：建立並設定連線，包含重試邏輯，只允許 connect() 呼叫
 * 這個寫法代表只能設定一個連線
 * 如果之後有多個 vhost 再來重構
 */
func (cm *ConnectionManager) setConnWithRetry() error {
	conn, err := backoff.Retry(
		context.Background(),
		cm.dialFunc,
		backoff.WithMaxElapsedTime(cm.Config.MaxElpasedTime),
		backoff.WithMaxTries(cm.Config.MaxRetries),
	)
	if err != nil {
		log.Println("setConnWithRetry failed, err:", err.Error())
		return err
	}

	cm.conn = conn
	return nil
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
