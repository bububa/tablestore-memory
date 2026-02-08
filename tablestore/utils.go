package tablestore

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/spf13/cast"

	"github.com/bububa/tablestore-memory/model"
)

func parseSessionFromRow(session *model.Session, columns []*tablestore.AttributeColumn, primaryKey *tablestore.PrimaryKey) {
	if primaryKey != nil {
		for _, col := range primaryKey.PrimaryKeys {
			switch col.ColumnName {
			case SessionUserIDField:
				session.UserID = cast.ToString(col.Value)
			case SessionSessionIDField:
				session.SessionID = cast.ToString(col.Value)
			case SessionUpdateTimeField:
				session.UpdateTime = cast.ToInt64(col.Value)
			}
		}
	}
	for _, col := range columns {
		switch col.ColumnName {
		case SessionUpdateTimeField:
			session.UpdateTime = cast.ToInt64(col.Value)
		case SessionSearchContentField:
			session.SearchContent = cast.ToString(col.Value)
		default:
			if session.Metadata == nil {
				session.Metadata = model.NewMetadata()
			}
			session.Metadata[col.ColumnName] = col.Value
		}
	}
}

func configBatchSize(batchSize int, iteratorMaxCount int, filter tablestore.ColumnFilter) int {
	if batchSize == -1 && iteratorMaxCount > 0 {
		if filter == nil {
			return max(min(5000, iteratorMaxCount), 1)
		} else {
			return max(min(5000, int(float64(iteratorMaxCount)*1.3)), 1)
		}
	}
	if batchSize <= 0 {
		return 5000
	}
	return batchSize
}

func parseMessageFromRow(message *model.Message, columns []*tablestore.AttributeColumn, primaryKey *tablestore.PrimaryKey) {
	if primaryKey != nil {
		for _, col := range primaryKey.PrimaryKeys {
			switch col.ColumnName {
			case MessageMessageIDField:
				message.MessageID = cast.ToString(col.Value)
			case MessageSessionIDField:
				message.SessionID = cast.ToString(col.Value)
			case MessageCreateTimeField:
				message.CreateTime = cast.ToInt64(col.Value)
			}
		}
	}
	for _, col := range columns {
		switch col.ColumnName {
		case MessageContentField:
			message.Content = cast.ToString(col.Value)
		case MessageSearchContentField:
			message.SearchContent = cast.ToString(col.Value)
		default:
			if message.Metadata == nil {
				message.Metadata = model.NewMetadata()
			}
			message.Metadata[col.ColumnName] = col.Value
		}
	}
}
