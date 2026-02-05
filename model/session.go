package model

// --------------------
// Session
// --------------------

type Session struct {
	UserID    string
	SessionID string

	UpdateTime int64
	Metadata   Metadata
}

// --------------------
// constructors
// --------------------

// NewSession creates a new session with current update time
func NewSession(userID, sessionID string) *Session {
	return NewSessionWithTime(userID, sessionID, CurrentTimeMicroseconds())
}

func NewSessionWithTime(userID, sessionID string, updateTime int64) *Session {
	return &Session{
		UserID:     userID,
		SessionID:  sessionID,
		UpdateTime: updateTime,
		Metadata:   NewMetadata(),
	}
}

func NewSessionFull(
	userID,
	sessionID string,
	updateTime int64,
	metadata Metadata,
) *Session {
	if metadata == nil {
		metadata = NewMetadata()
	}
	return &Session{
		UserID:     userID,
		SessionID:  sessionID,
		UpdateTime: updateTime,
		Metadata:   metadata,
	}
}

// Clone copy constructor
func (s *Session) Clone() *Session {
	if s == nil {
		return nil
	}
	cp := *s
	cp.Metadata = s.Metadata.Copy()
	return &cp
}

// --------------------
// fluent setters
// --------------------
func (s *Session) SetUpdateTime(t int64) *Session {
	s.UpdateTime = t
	return s
}

func (s *Session) SetMetadata(metadata Metadata) *Session {
	if metadata == nil {
		metadata = NewMetadata()
	}
	s.Metadata = metadata
	return s
}

// --------------------
// behavior
// --------------------

// RefreshUpdateTime updates the update time to now (microseconds)
func (s *Session) RefreshUpdateTime() {
	s.UpdateTime = CurrentTimeMicroseconds()
}
