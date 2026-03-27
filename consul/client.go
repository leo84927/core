package consul

import (
	"github.com/hashicorp/consul/api"
	"github.com/rotisserie/eris"
)

type Client struct {
	checkId string
	kv      *api.KV
	agent   *api.Agent
}

func NewClient() (*Client, error) {
	// DefaultConfig() 會自動用 os.Getenv 取得 CONSUL_HTTP_ADDR 環境變數，要使用 consul 的服務要記得先設定
	client, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		return nil, eris.Cause(err)
	}

	return &Client{
		kv:    client.KV(),
		agent: client.Agent(),
	}, nil
}
