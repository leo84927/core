package rabbitmq

import (
	"fmt"
	"log"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rotisserie/eris"
)

var (
	conn *amqp.Connection
)

type Config struct {
	ServiceName string
	User        string
	Password    string
	Host        string
	Port        string
	Vhost       string
}

/**
 * 這個寫法代表只能設定一個連線
 * 如果之後有多個 vhost 再來重構
 */
func SetConn(config Config) {
	var err error

	conn, err = amqp.DialConfig(
		buildUrl(config),
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
		log.Println("SetConn failed, err:", err.Error())
		panic(err)
	}
}

func GetConn() *amqp.Connection {
	if conn == nil {
		panic(eris.New("Please call SetConn first"))
	}
	return conn
}

func Close() {
	err := conn.Close()
	if err != nil {
		panic(err)
	}
}

func buildUrl(config Config) string {
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
