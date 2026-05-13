package mariadb

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
)

type DataSourceName struct {
	User         string
	Password     string
	Host         string
	Port         string
	DatabaseName string
	Charset      string
	Collation    string
	Timeout      string // 一次完整 SQL 中，對 TCP 認證的限時
	ReadTimeout  string // 一次完整 SQL 中，執行 conn.Read 後，到資料進入 OS recv buffer 為止的限時
	WriteTimeout string // 一次完整 SQL 中，發送 SQL 到 OS send buffer 的限時

	MaxOpenConns    int           // 最多同時開啟幾條連線（包含 idle + in-use），超過此數的請求會排隊等待。設太小會成為瓶頸，設太大會壓垮資料庫。
	MaxIdleConns    int           // 連線池中最多保留幾條閒置連線。閒置連線可以被下一次請求直接重用，避免重新建立連線的開銷。建議設為 MaxOpenConns 的一半以下。
	ConnMaxLifetime time.Duration // 一條連線最長可以存活多久，超過後會被強制關閉並重建。可避免使用到因網路問題或 server 設定（wait_timeout）而已經失效的舊連線。
	ConnMaxIdleTime time.Duration // 一條閒置連線最久可以放多久，超過後會被回收。在流量不穩定的服務中，可避免連線池中堆積大量用不到的閒置連線。
}

type Config struct {
	WriteDB        DataSourceName
	ReadDB         DataSourceName
	MaxRetries     uint          // 最大重試次數上限
	MaxElapsedTime time.Duration // 總重試時間上限
}

func (d DataSourceName) buildDB() (*sqlx.DB, error) {
	db, err := sqlx.Open("mysql", d.buildDSN())
	if err != nil {
		// sqlx.Open 只有在 dsn 格式錯誤時才會失敗
		return nil, permanentIfNeeded(err)
	}

	db.SetMaxOpenConns(d.MaxOpenConns)
	db.SetMaxIdleConns(d.MaxIdleConns)
	db.SetConnMaxLifetime(d.ConnMaxLifetime)
	db.SetConnMaxIdleTime(d.ConnMaxIdleTime)

	// 實際連線
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, permanentIfNeeded(err)
	}

	return db, nil
}

func (d DataSourceName) buildDSN() string {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=%s&parseTime=True&loc=UTC&collation=%s&timeout=%s&readTimeout=%s&writeTimeout=%s",
		d.User,
		d.Password,
		d.Host,
		d.Port,
		d.DatabaseName,
		d.Charset,
		d.Collation,
		d.Timeout,
		d.ReadTimeout,
		d.WriteTimeout,
	)

	slog.Debug(
		"buildDSN finish",
		"dsn", dsn,
	)
	return dsn
}
