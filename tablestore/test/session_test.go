package test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/bububa/tablestore-memory/model"
)

func TestSessionStore(t *testing.T) {
	store := MemoryStore()
	if err := store.InitTable(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteAllSessions(); err != nil {
		t.Error(err)
	}
	session := randomSession("user_1")
	if err := store.PutSession(session); err != nil {
		t.Error(err)
	}
	sessionPut := new(model.Session)
	sessionPut.SessionID = session.SessionID
	sessionPut.UserID = session.UserID
	if err := store.GetSession(sessionPut); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(sessionPut, session) {
		t.Fatalf("sessions not equal:\n%+v\n%+v", sessionPut, session)
	}
	if !session.Metadata.HasKey("meta_example_string") {
		t.Error("expect metadata has key: 'meta_example_string'")
	}
	if !session.Metadata.HasKey("meta_example_text") {
		t.Error("expect metadata has key: 'meta_example_text'")
	}
	if !session.Metadata.HasKey("meta_example_long") {
		t.Error("expect metadata has key: 'meta_example_long'")
	}
	if !session.Metadata.HasKey("meta_example_double") {
		t.Error("expect metadata has key: 'meta_example_double'")
	}
	if !session.Metadata.HasKey("meta_example_boolean") {
		t.Error("expect metadata has key: 'meta_example_boolean'")
	}
	if !session.Metadata.HasKey("meta_example_bytes") {
		t.Error("expect metadata has key: 'meta_example_bytes'")
	}
	sessionToUpdate := session.Clone()
	if !reflect.DeepEqual(sessionToUpdate, session) {
		t.Fatalf("sessions not equal:\n%+v\n%+v", sessionToUpdate, session)
	}
	sessionToUpdate.Metadata.Put("meta_example_string", "updated")
	if err := store.UpdateSession(sessionToUpdate); err != nil {
		t.Error(err)
	}
	sessionUpdated := new(model.Session)
	sessionUpdated.SessionID = sessionToUpdate.SessionID
	sessionUpdated.UserID = "user_1"
	if err := store.GetSession(sessionUpdated); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(sessionUpdated, sessionToUpdate) {
		t.Fatalf("sessions not equal:\n%+v\n%+v", sessionUpdated, sessionToUpdate)
	}
	if err := store.DeleteSession("user_1", sessionUpdated.SessionID); err != nil {
		t.Error(err)
	} else if err := store.GetSession(session); err == nil {
		t.Error("expect session deleted, but not")
	}
	var (
		total      = 80
		user1Count int
		user2Count int
	)
	for range total {
		sessionForDelete := randomSession(randomFrom([]string{"user_for_delete_1", "user_for_delete_2"}))
		switch sessionForDelete.UserID {
		case "user_for_delete_1":
			user1Count += 1
		case "user_for_delete_2":
			user2Count += 1
		}
		if err := store.PutSession(sessionForDelete); err != nil {
			t.Error(err)
		}
	}
	t.Logf("total:%d, user1Count:%d, user2Count:%d", total, user1Count, user2Count)
	var count int
	for range store.ListAllSessions() {
		count += 1
	}
	if total != count {
		t.Errorf("expect total sessions:%d, got:%d", total, count)
	}
	if n, err := store.DeleteSessions("user_for_delete_1"); err != nil {
		t.Error(err)
	} else if user1Count != n {
		t.Errorf("expect delete user1 sessions:%d, got:%d", user1Count, n)
	}
	if n, err := store.DeleteSessions("user_for_delete_2"); err != nil {
		t.Error(err)
	} else if user2Count != n {
		t.Errorf("expect delete user2 sessions:%d, got:%d", user2Count, n)
	}
	for range total {
		sessionForDelete := randomSession(randomFrom([]string{"user_for_delete_1", "user_for_delete_2"}))
		if err := store.PutSession(sessionForDelete); err != nil {
			t.Error(err)
		}
	}
	count = 0
	for range store.ListAllSessions() {
		count += 1
	}
	if total != count {
		t.Errorf("expect total sessions:%d, got:%d", total, count)
	}
	if n, err := store.DeleteAllSessions(); err != nil {
		t.Error(err)
	} else if total != n {
		t.Errorf("expect delete sessions:%d, got:%d", total, n)
	}
	count = 0
	for range store.ListAllSessions() {
		count += 1
	}
	if count > 0 {
		t.Errorf("expect exists sessions:0, got:%d", count)
	}
	session = randomSession("user_1")
	session.SetUpdateTime(123)
	session.SetMetadata(nil)
	if err := store.PutSession(session); err != nil {
		t.Error(err)
	}
	if err := store.UpdateSession(session); err != nil {
		t.Error(err)
	}
	emptySessionRead := new(model.Session)
	emptySessionRead.UserID = "user_1"
	emptySessionRead.SessionID = session.SessionID
	if err := store.GetSession(emptySessionRead); err != nil {
		t.Error(err)
	}
	if emptySessionRead.UserID != "user_1" {
		t.Errorf("expect session user_id:user_1, got:%s", emptySessionRead.UserID)
	}
	if session.SessionID != emptySessionRead.SessionID {
		t.Errorf("expect session id:%s, got:%s", session.SessionID, emptySessionRead.SessionID)
	}
	if emptySessionRead.UpdateTime != 123 {
		t.Errorf("expect session update_time:123, got:%d", emptySessionRead.UpdateTime)
	}
	if n := emptySessionRead.Metadata.Size(); n != 0 {
		t.Errorf("expect session metadata is empty, got:%d", n)
	}
	if err := store.DeleteSession(session.UserID, session.SessionID); err != nil {
		t.Error(err)
	}
}

func TestSessionSearch(t *testing.T) {
	store := MemoryStore()
	if err := store.InitTable(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteAllSessions(); err != nil {
		t.Error(err)
	}
	var total int64 = 100
	for idx := range total {
		sessionForSearch := randomSession("user_search")
		sessionForSearch.SetSearchContent(fmt.Sprintf("test searchable, item_%d", idx))
		if err := store.PutSession(sessionForSearch); err != nil {
			t.Error(err)
		}
	}
	time.Sleep(time.Second * 11)
	if resp, err := store.SearchSessions("user_search", "searchable", 0, 0, int32(total), nil); err != nil {
		t.Error(err)
	} else if resp.Total != total {
		t.Errorf("expected search results:%d, got:%d", total, resp.Total)
	}
	if resp, err := store.SearchSessions("user_search1", "searchable", 0, 0, int32(total), nil); err != nil {
		t.Error(err)
	} else if resp.Total != 0 {
		t.Errorf("expected search results:0, got:%d", resp.Total)
	}
	if resp, err := store.SearchSessions("user_search", "xxxxx", 0, 0, int32(total), nil); err != nil {
		t.Error(err)
	} else if resp.Total != 0 {
		t.Errorf("expected search results:0, got:%d", resp.Total)
	}
	if resp, err := store.SearchSessions("user_search", "item_1", 0, 0, int32(total), nil); err != nil {
		t.Error(err)
	} else if resp.Total != 11 {
		t.Errorf("expected search results:11, got:%d", resp.Total)
	}
	if _, err := store.DeleteAllSessions(); err != nil {
		t.Error(err)
	}
}
