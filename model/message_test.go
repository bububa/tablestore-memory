package model

import (
	"reflect"
	"testing"
)

func TestMessage_Simple(t *testing.T) {
	metadata := NewMetadata()
	metadata.Put("key", "value")
	metadata.Put("key2", 1)

	// simple constructor with createTime
	messageSimple1 := NewMessageWithTime("123", "456", 123)
	messageSimple1.SetContent("hello world")
	messageSimple1.SetMetadata(metadata)

	// simple constructor without createTime
	messageSimple2 := NewMessage("123", "456")
	messageSimple2.SetCreateTime(123)
	messageSimple2.SetContent("hello world")
	messageSimple2.SetMetadata(metadata)

	// copy constructor
	messageCopy := messageSimple2.Clone()

	// equality checks
	if !reflect.DeepEqual(messageCopy, messageSimple2) {
		t.Fatalf("messages not equal:\n%+v\n%+v", messageCopy, messageSimple2)
	}
}
