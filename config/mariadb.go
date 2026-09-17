package config

import (
	"time"

	env "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/env"
	"github.com/leo84927/core/v2/mariadb"
)

// 只有宣告 Spec.MariaDB 的服務才讀這 14 個鍵
func (r *reader) mariaDB() *mariadb.Config {
	// 讀寫兩庫除了 host 之外完全同值，共用的部分先讀一次
	dsn := mariadb.DataSourceName{
		User:            r.str(env.GlobalEnvKey_GLOBAL_MARIADB_USER),
		Password:        r.str(env.GlobalEnvKey_GLOBAL_MARIADB_PASSWORD),
		Port:            r.str(env.GlobalEnvKey_GLOBAL_MARIADB_PORT),
		DatabaseName:    r.str(env.GlobalEnvKey_GLOBAL_MARIADB_DATABASE_NAME),
		Charset:         r.str(env.GlobalEnvKey_GLOBAL_MARIADB_CHARSET),
		Collation:       r.str(env.GlobalEnvKey_GLOBAL_MARIADB_COLLATION),
		Timeout:         r.str(env.GlobalEnvKey_GLOBAL_MARIADB_TIMEOUT),
		ReadTimeout:     r.str(env.GlobalEnvKey_GLOBAL_MARIADB_READ_TIMEOUT),
		WriteTimeout:    r.str(env.GlobalEnvKey_GLOBAL_MARIADB_WRITE_TIMEOUT),
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: 5 * time.Minute,
		ConnMaxIdleTime: 1 * time.Minute,
		// 空值有語意：本地開發不開 TLS，見 mariadb.DataSourceName.buildDB
		TLSCaPEM: r.str(env.GlobalEnvKey_GLOBAL_MARIADB_TLS_CA_PEM),
	}

	writeDB := dsn
	writeDB.Host = r.str(env.GlobalEnvKey_GLOBAL_MARIADB_PRIMARY_HOST) // 寫 - 主庫

	readDB := dsn
	readDB.Host = r.str(env.GlobalEnvKey_GLOBAL_MARIADB_REPLICA_HOST) // 讀 - 從庫

	return &mariadb.Config{
		WriteDB:        writeDB,
		ReadDB:         readDB,
		MaxRetries:     r.uint(env.GlobalEnvKey_GLOBAL_MARIADB_CONN_MAX_RETRIES),
		MaxElapsedTime: r.duration(env.GlobalEnvKey_GLOBAL_MARIADB_CONN_MAX_ELAPSED_TIME),
	}
}
