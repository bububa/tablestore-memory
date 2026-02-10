package model

// --------------------
// Message
// --------------------

type Message struct {
	SessionID  string `json:"session_id,omitempty"`
	MessageID  string `json:"message,omitempty"`
	CreateTime int64  `json:"create_time,omitempty"`

	Content       string   `json:"content,omitempty"`
	Metadata      Metadata `json:"metadata,omitempty"`
	SearchContent string   `json:"search_content,omitempty"`
}

// --------------------
// constructors
// --------------------

// NewMessage creates a message with current time (microseconds)
func NewMessage(sessionID, messageID string) *Message {
	return NewMessageWithTime(sessionID, messageID, CurrentTimeMicroseconds())
}

func NewMessageWithTime(sessionID, messageID string, createTime int64) *Message {
	return &Message{
		SessionID:  sessionID,
		MessageID:  messageID,
		CreateTime: createTime,
		Metadata:   NewMetadata(),
	}
}

func NewMessageFull(
	sessionID,
	messageID string,
	createTime int64,
	content string,
	metadata Metadata,
) *Message {
	if metadata == nil {
		metadata = NewMetadata()
	}
	return &Message{
		SessionID:  sessionID,
		MessageID:  messageID,
		CreateTime: createTime,
		Content:    content,
		Metadata:   metadata,
	}
}

// Clone copy constructor
func (m *Message) Clone() *Message {
	if m == nil {
		return nil
	}
	cp := *m
	cp.Metadata = m.Metadata.Copy()
	return &cp
}

// --------------------
// fluent setters (chainable)
// --------------------
func (m *Message) SetCreateTime(t int64) *Message {
	m.CreateTime = t
	return m
}

func (m *Message) SetContent(content string) *Message {
	m.Content = content
	return m
}

func (m *Message) SetSearchContent(content string) *Message {
	m.SearchContent = content
	return m
}

func (m *Message) SetMetadata(metadata Metadata) *Message {
	if metadata == nil {
		metadata = NewMetadata()
	}
	m.Metadata = metadata
	return m
}
