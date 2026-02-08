package tablestore

const (
	DefaultSessionTableName          = "session"
	DefaultSessionSecondaryIndexName = "session_secondary_index"
	DefaultSessionSearchIndexName    = "session_search_index"
	DefaultMessageTableName          = "message"
	DefaultMessageSearchIndexName    = "message_search_index"
	DefaultMessageSecondaryIndexName = "message_secondary_index"
)

const (
	SessionUserIDField        = "user_id"
	SessionSessionIDField     = "session_id"
	SessionUpdateTimeField    = "update_time"
	SessionSearchContentField = "search_content"
)

const (
	MessageSessionIDField     = "session_id"
	MessageMessageIDField     = "message_id"
	MessageCreateTimeField    = "create_time"
	MessageContentField       = "content"
	MessageSearchContentField = "search_content"
)
