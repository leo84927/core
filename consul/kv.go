package consul

import "github.com/rotisserie/eris"

func (c *Client) List(prefix string) (map[string]string, error) {
	pairs, _, err := c.kv.List(prefix, nil)
	if err != nil {
		return nil, eris.Cause(err)
	}

	result := make(map[string]string, len(pairs))
	for _, kv := range pairs {
		result[kv.Key] = string(kv.Value)
	}

	return result, nil
}
