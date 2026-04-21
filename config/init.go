package config

import (
	"log"
	"maps"
	"time"

	cp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/consul"
	"github.com/leo84927/core/consul"
)

type ServiceConfig struct {
	ServicePrefix string
	EnvKeys       []string
}

func NewServiceConfig(prefix string, envKeys []string) *ServiceConfig {
	return &ServiceConfig{
		ServicePrefix: prefix,
		EnvKeys:       envKeys,
	}
}

func (sc *ServiceConfig) InitFromConsul() {
	var err error

	// 建立 consul client，失敗時要直接 panic
	Client, err = consul.NewClient()
	if err != nil {
		log.Fatalf("new consul client failed, err: %v\n", err)
	}

	// 透過 consul 取得 global 環境變數，此時 EnvMap 為空可以直接指定
	if EnvMap, err = Client.List("GLOBAL"); err != nil {
		log.Fatalf("get env from consul failed, err: %v\n", err)
	}

	// 取得服務相關的環境變數，遍歷後指定給 EnvMap
	if serviceMap, err := Client.List(sc.ServicePrefix); err != nil {
		log.Fatalf("get env from consul failed, err: %v\n", err)
	} else {
		maps.Copy(EnvMap, serviceMap)
	}

	// 指定 alloy
	AlloyHost = EnvMap[cp.GlobalEnvKey_GLOBAL_ALLOY_HOST.String()]
	AlloyPort = EnvMap[cp.GlobalEnvKey_GLOBAL_ALLOY_PORT.String()]

	// 指定時區
	TimeZone = EnvMap[cp.GlobalEnvKey_GLOBAL_TIMEZONE.String()]
	if Loc, err = time.LoadLocation(TimeZone); err != nil {
		log.Fatalf("load location failed, err: %v\n", err)
	}
}
