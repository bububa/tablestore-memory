package test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/bububa/tablestore-memory/model"
)

func TestMessageStore(t *testing.T) {
	store := MemoryStore()
	if err := store.InitTable(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteAllMessages(); err != nil {
		t.Error(err)
	}
	var createTime int64 = 123
	message := randomMessage("session_1")
	message.SetCreateTime(createTime)
	if err := store.PutMessage(message); err != nil {
		t.Error(err)
	}
	messagePut := new(model.Message)
	messagePut.SessionID = "session_1"
	messagePut.MessageID = message.MessageID
	if err := store.GetMessage(messagePut); err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(messagePut, message) {
		t.Fatalf("messages not equal:\n%+v\n%+v", messagePut, message)
	}
	if !message.Metadata.HasKey("meta_example_string") {
		t.Error("expect metadata has key: 'meta_example_string'")
	}
	if !message.Metadata.HasKey("meta_example_text") {
		t.Error("expect metadata has key: 'meta_example_text'")
	}
	if !message.Metadata.HasKey("meta_example_long") {
		t.Error("expect metadata has key: 'meta_example_long'")
	}
	if !message.Metadata.HasKey("meta_example_double") {
		t.Error("expect metadata has key: 'meta_example_double'")
	}
	if !message.Metadata.HasKey("meta_example_boolean") {
		t.Error("expect metadata has key: 'meta_example_boolean'")
	}
	if !message.Metadata.HasKey("meta_example_bytes") {
		t.Error("expect metadata has key: 'meta_example_bytes'")
	}

	messageToUpdate := message.Clone()
	if !reflect.DeepEqual(messageToUpdate, message) {
		t.Fatalf("messages not equal:\n%+v\n%+v", messageToUpdate, message)
	}
	messageToUpdate.Metadata.Put("meta_example_string", "updated")
	if err := store.UpdateMessage(messageToUpdate); err != nil {
		t.Error(err)
	}
	messageUpdated := new(model.Message)
	messageUpdated.MessageID = messageToUpdate.MessageID
	messageUpdated.SessionID = "session_1"
	if err := store.GetMessage(messageUpdated); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(messageUpdated, messageToUpdate) {
		t.Fatalf("messages not equal:\n%+v\n%+v", messageUpdated, messageToUpdate)
	}
	if err := store.DeleteMessage("session_1", messageUpdated.MessageID, 0); err != nil {
		t.Error(err)
	} else if err := store.GetMessage(message); err == nil {
		t.Error("expect message deleted, but not")
	}
	var (
		total         = 80
		session1Count int
		session2Count int
	)
	for range total {
		messageForDelete := randomMessage(randomFrom([]string{"session_for_delete_1", "session_for_delete_2"}))
		switch messageForDelete.SessionID {
		case "session_for_delete_1":
			session1Count += 1
		case "session_for_delete_2":
			session2Count += 1
		}
		if err := store.PutMessage(messageForDelete); err != nil {
			t.Error(err)
		}
	}
	t.Logf("total:%d, session1Count:%d, session2Count:%d", total, session1Count, session2Count)
	var count int
	for range store.ListAllMessages() {
		count += 1
	}
	if total != count {
		t.Errorf("expect total messages:%d, got:%d", total, count)
	}
	if n, err := store.DeleteMessages("session_for_delete_1"); err != nil {
		t.Error(err)
	} else if session1Count != n {
		t.Errorf("expect delete session1 sessions:%d, got:%d", session1Count, n)
	}
	if n, err := store.DeleteMessages("session_for_delete_2"); err != nil {
		t.Error(err)
	} else if session2Count != n {
		t.Errorf("expect delete session2 sessions:%d, got:%d", session2Count, n)
	}
	for range total {
		messageForDelete := randomMessage(randomFrom([]string{"session_for_delete_1", "session_for_delete_2"}))
		if err := store.PutMessage(messageForDelete); err != nil {
			t.Error(err)
		}
	}
	count = 0
	for range store.ListAllMessages() {
		count += 1
	}
	if total != count {
		t.Errorf("expect total messages:%d, got:%d", total, count)
	}
	if n, err := store.DeleteAllMessages(); err != nil {
		t.Error(err)
	} else if total != n {
		t.Errorf("expect delete messages:%d, got:%d", total, n)
	}
	count = 0
	for range store.ListAllMessages() {
		count += 1
	}
	if count > 0 {
		t.Errorf("expect exists messages:0, got:%d", count)
	}
	message = randomMessage("session_1")
	message.SetContent("")
	message.SetMetadata(nil)
	if err := store.PutMessage(message); err != nil {
		t.Error(err)
	}
	if err := store.UpdateMessage(message); err != nil {
		t.Error(err)
	}
	emptyMessageRead := new(model.Message)
	emptyMessageRead.SessionID = "session_1"
	emptyMessageRead.MessageID = message.MessageID
	if err := store.GetMessage(emptyMessageRead); err != nil {
		t.Error(err)
	}
	if emptyMessageRead.SessionID != "session_1" {
		t.Errorf("expect message session_id:session_1, got:%s", emptyMessageRead.SessionID)
	}
	if message.MessageID != emptyMessageRead.MessageID {
		t.Errorf("expect message id:%s, got:%s", message.MessageID, emptyMessageRead.MessageID)
	}
	if message.CreateTime != emptyMessageRead.CreateTime {
		t.Errorf("expect message create_time:%d, got:%d", message.CreateTime, emptyMessageRead.CreateTime)
	}
	if emptyMessageRead.Content != "" {
		t.Errorf("expect empty message content, got:%s", emptyMessageRead.Content)
	}
	if n := emptyMessageRead.Metadata.Size(); n != 0 {
		t.Errorf("expect message metadata is empty, got:%d", n)
	}
	if err := store.DeleteMessage(message.SessionID, message.MessageID, message.CreateTime); err != nil {
		t.Error(err)
	}
}

func TestMessageSearch(t *testing.T) {
	store := MemoryStore()
	if err := store.InitTable(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteAllMessages(); err != nil {
		t.Error(err)
	}
	var total int64 = 100
	for idx := range total {
		messageForSearch := randomMessage("session_search")
		messageForSearch.SetSearchContent(fmt.Sprintf("test searchable, item_%d", idx))
		if err := store.PutMessage(messageForSearch); err != nil {
			t.Error(err)
		}
	}
	time.Sleep(time.Second * 11)
	if resp, err := store.SearchMessages("session_search", "searchable", 0, 0, int32(total), nil); err != nil {
		t.Error(err)
	} else if resp.Total != total {
		t.Errorf("expected search results:%d, got:%d", total, resp.Total)
	}
	if resp, err := store.SearchMessages("session_search1", "searchable", 0, 0, int32(total), nil); err != nil {
		t.Error(err)
	} else if resp.Total != 0 {
		t.Errorf("expected search results:0, got:%d", resp.Total)
	}
	if resp, err := store.SearchMessages("session_search", "xxxxx", 0, 0, int32(total), nil); err != nil {
		t.Error(err)
	} else if resp.Total != 0 {
		t.Errorf("expected search results:0, got:%d", resp.Total)
	}
	if resp, err := store.SearchMessages("session_search", "item_1", 0, 0, int32(total), nil); err != nil {
		t.Error(err)
	} else if resp.Total != 11 {
		t.Errorf("expected search results:11, got:%d", resp.Total)
	}
	if _, err := store.DeleteAllMessages(); err != nil {
		t.Error(err)
	}
}
