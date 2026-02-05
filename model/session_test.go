package model

import (
	"math"
	"reflect"
	"testing"
)

func TestSession_Simple(t *testing.T) {
	metadata := NewMetadata().
		Put("key", "value").
		Put("key2", 1)

	sessionSimple1 := NewSessionWithTime("test_user_123", "test_session_123", CurrentTimeMicroseconds())
	sessionSimple1.SetMetadata(metadata)

	sessionSimple2 := NewSession("test_user_456", "test_session_456")
	sessionSimple2.SetUpdateTime(CurrentTimeMicroseconds())
	sessionSimple2.SetMetadata(metadata)

	sessionSimple3 := NewSessionFull("test_user_789", "test_session_789", CurrentTimeMicroseconds(), metadata)

	// equality checks

	// refreshUpdateTime
	sessionSimple3.RefreshUpdateTime()
	nowMicros := CurrentTimeMicroseconds()
	diff := math.Abs(float64(sessionSimple3.UpdateTime - nowMicros))
	if diff >= 100_000 {
		t.Fatalf("updateTime diff too large: %f", diff)
	}

	sessionCopy := sessionSimple3.Clone()
	// equality checks
	if !reflect.DeepEqual(sessionCopy, sessionSimple3) {
		t.Fatalf("sessions not equal:\n%+v\n%+v", sessionCopy, sessionSimple3)
	}
}
