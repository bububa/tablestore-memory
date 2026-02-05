package tablestore

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"

	"github.com/bububa/tablestore-memory/model"
	"github.com/bububa/tablestore-memory/protocol"
)

type MemoryStore struct {
	model.Options
	clt *tablestore.TableStoreClient
}

func NewMemoryStore(clt *tablestore.TableStoreClient, opts ...model.Option) *MemoryStore {
	ret := &MemoryStore{
		clt: clt,
	}
	for _, opt := range opts {
		opt(&ret.Options)
	}
	if ret.SessionTableName == "" {
		ret.SessionTableName = DefaultSessionTableName
	}
	if ret.MessageTableName == "" {
		ret.MessageTableName = DefaultMessageTableName
	}
	if ret.SessionSecondaryIndexName == "" {
		ret.SessionSecondaryIndexName = DefaultSessionSecondaryIndexName
	}
	if ret.MessageSecondaryIndexName == "" {
		ret.MessageSecondaryIndexName = DefaultMessageSecondaryIndexName
	}
	return ret
}

var _ protocol.MemoryStore = (*MemoryStore)(nil)

func (s *MemoryStore) InitTable() error {
	if err := s.InitSessionTable(); err != nil {
		return err
	}
	return s.InitMessageTable()
}

// DeleteSessionAndMessages delete a session and its messages
func (s *MemoryStore) DeleteSessionAndMessages(userID, sessionID string) error {
	if err := s.DeleteSession(userID, sessionID); err != nil {
		return err
	}
	if _, err := s.DeleteMessages(sessionID); err != nil {
		return err
	}
	return nil
}
