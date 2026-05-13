package consul

import (
	"strings"

	"github.com/rotisserie/eris"
)

func (c *Client) List(prefix string) (map[string]string, error) {
	pairs, _, err := c.kv.List(prefix, nil)
	if err != nil {
		return nil, eris.Cause(err)
	}

	result := make(map[string]string, len(pairs))
	for _, kv := range pairs {
		key := strings.ReplaceAll(kv.Key, "/", "_")
		result[key] = string(kv.Value)
	}

	return result, nil
}
