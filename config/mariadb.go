package config

import (
	"log"
	"strconv"
	"time"

	env "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/env"
	"github.com/leo84927/core/mariadb"
)

var mariadbCfg mariadb.Config

func LoadMariaDbConfig() {
	connMaxRetries, err := strconv.Atoi(EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_CONN_MAX_RETRIES.String()])
	if err != nil {
		log.Printf("transfer MARIADB_CONN_MAX_RETRIES failed, invalid value: %v, error: %v\n", EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_CONN_MAX_RETRIES.String()], err)
		connMaxRetries = 5
	}
	connMaxElapsed, err := time.ParseDuration(EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_CONN_MAX_ELAPSED_TIME.String()])
	if err != nil {
		log.Printf("transfer MARIADB_CONN_MAX_ELAPSED_TIME failed, invalid value: %v, error: %v\n", EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_CONN_MAX_ELAPSED_TIME.String()], err)
		connMaxElapsed = 20 * time.Second
	}

	user := EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_USER.String()]
	password := EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_PASSWORD.String()]
	port := EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_PORT.String()]
	databaseName := EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_DATABASE_NAME.String()]
	charset := EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_CHARSET.String()]
	collation := EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_COLLATION.String()]
	timeout := EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_TIMEOUT.String()]
	readTimeout := EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_READ_TIMEOUT.String()]
	writeTimeout := EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_WRITE_TIMEOUT.String()]
	tlsCaPEM := EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_TLS_CA_PEM.String()]

	mariadbCfg = mariadb.Config{
		WriteDB: mariadb.DataSourceName{
			User:         user,
			Password:     password,
			Host:         EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_PRIMARY_HOST.String()], // 寫 - 主庫
			Port:         port,
			DatabaseName: databaseName,
			Charset:      charset,
			Collation:    collation,
			Timeout:      timeout,
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
			TLSCaPEM:     tlsCaPEM,
		},
		ReadDB: mariadb.DataSourceName{
			User:         user,
			Password:     password,
			Host:         EnvMap[env.GlobalEnvKey_GLOBAL_MARIADB_REPLICA_HOST.String()], // 讀 - 從庫
			Port:         port,
			DatabaseName: databaseName,
			Charset:      charset,
			Collation:    collation,
			Timeout:      timeout,
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
			TLSCaPEM:     tlsCaPEM,
		},
		MaxRetries:     uint(connMaxRetries),
		MaxElapsedTime: connMaxElapsed,
	}
}

func GetMariaDbConfig() mariadb.Config {
	return mariadbCfg
}
