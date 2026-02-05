package test

import (
	"os"

	"github.com/bububa/tablestore-memory/client"
	"github.com/bububa/tablestore-memory/protocol"
)

func MemoryStore() protocol.MemoryStore {
	cfg := client.Config{
		Endpoint:        os.Getenv("OTS_ENDPOINT"),
		Instance:        os.Getenv("OTS_INSTANCE"),
		AccessKeyID:     os.Getenv("OTS_AK"),
		AccessKeySecret: os.Getenv("OTS_SK"),
	}
	store, err := client.NewMemoryStore(&cfg)
	if err != nil {
		panic(err)
	}
	return store
}
