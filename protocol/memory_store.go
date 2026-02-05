package protocol

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"

	"github.com/bububa/tablestore-memory/model"
)

// MemoryStore defines the interface for session and message persistence.
type MemoryStore interface {
	// <-------- Session related -------->

	// PutSession insert (overwrite) a session
	PutSession(session *model.Session) error

	// UpdateSession update a session
	UpdateSession(session *model.Session) error

	// DeleteSession delete a session
	DeleteSession(userID, sessionID string) error

	// DeleteSessions delete all sessions for a user
	DeleteSessions(userID string) (int, error)

	// DeleteSessionAndMessages delete a session and its messages
	DeleteSessionAndMessages(userID, sessionID string) error

	// DeleteAllSessions delete all sessions for all users
	DeleteAllSessions() (int, error)

	// GetSession get a session
	GetSession(session *model.Session) error

	// ListAllSessions list all sessions
	ListAllSessions() <-chan model.Session

	// ListSessions list sessions for a specific user
	ListSessions(
		userID string,
		filter tablestore.ColumnFilter,
		maxCount int,
		batchSize int,
	) <-chan model.Session

	// ListRecentSessions list recent sessions sorted by update time
	ListRecentSessions(
		userID string,
		filter tablestore.ColumnFilter,
		inclusiveStartUpdateTime int64,
		inclusiveEndUpdateTime int64,
		maxCount int,
		batchSize int,
	) ([]model.Session, error)

	// ListRecentSessionsPaginated paginated recent sessions
	ListRecentSessionsPaginated(
		userID string,
		filter tablestore.ColumnFilter,
		inclusiveStartUpdateTime int64,
		inclusiveEndUpdateTime int64,
		pageSize int,
		nextStartPrimaryKey *tablestore.PrimaryKey,
	) (*model.Response[model.Session], error)

	// <-------- Message related -------->

	// PutMessage insert (overwrite) a message
	PutMessage(message *model.Message) error

	// UpdateMessage update a message
	UpdateMessage(message *model.Message) error

	// DeleteMessage delete a message
	DeleteMessage(sessionID string, messageID string, createTime int64) error

	// DeleteMessages delete all messages for a session
	DeleteMessages(sessionID string) (int, error)

	// DeleteAllMessages delete all messages
	DeleteAllMessages() (int, error)

	// GetMessage get a message
	GetMessage(message *model.Message) error

	// ListAllMessages list all messages
	ListAllMessages() <-chan model.Message

	// ListMessages list messages for a session
	ListMessages(sessionID string) <-chan model.Message

	// ListMessagesWithFilter  list messages with filters
	ListMessagesWithFilter(
		sessionID string,
		filter tablestore.ColumnFilter,
		inclusiveStartCreateTime int64,
		inclusiveEndCreateTime int64,
		order tablestore.Direction,
		maxCount int,
		batchSize int,
	) <-chan model.Message

	// ListMessagesPaginated  paginated messages
	ListMessagesPaginated(
		sessionID string,
		filter tablestore.ColumnFilter,
		inclusiveStartCreateTime int64,
		inclusiveEndCreateTime int64,
		order tablestore.Direction,
		pageSize int,
		nextStartPrimaryKey *tablestore.PrimaryKey,
	) (*model.Response[model.Message], error)

	// <-------- Infra -------->

	// InitTable initialize table
	InitTable() error

	// InitSearchIndex initialize search index
	// InitSearchIndex() error

	// DeleteTableAndIndex delete table and index
	// DeleteTableAndIndex() error
}
