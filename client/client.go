package client

import (
	"fmt"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"

	"github.com/bububa/tablestore-memory/model"
	"github.com/bububa/tablestore-memory/protocol"
	tb "github.com/bububa/tablestore-memory/tablestore"
)

func NewMemoryStore(cfg *Config, opts ...model.Option) (protocol.MemoryStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config must not be nil")
	}
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("config.Endpoint is required")
	}
	if cfg.Instance == "" {
		return nil, fmt.Errorf("config.Instance is required")
	}
	if cfg.AccessKeyID == "" {
		return nil, fmt.Errorf("config.AccessKeyID is required")
	}
	if cfg.AccessKeySecret == "" {
		return nil, fmt.Errorf("config.AccessKeySecret is required")
	}

	clt := tablestore.NewClientWithConfig(cfg.Endpoint, cfg.Instance, cfg.AccessKeyID, cfg.AccessKeySecret, cfg.SecurityToken, nil)
	return tb.NewMemoryStore(clt, opts...), nil
}
