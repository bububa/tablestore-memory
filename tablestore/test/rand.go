package test

import (
	"math/rand"
	"strings"

	"github.com/go-faker/faker/v4"
	"github.com/google/uuid"

	"github.com/bububa/tablestore-memory/model"
)

func randomSession(userID string) *model.Session {
	session := model.NewSession(userID, uuid.NewString())
	session.SetUpdateTime(rand.Int63n(100))

	name := faker.Name()

	session.Metadata.Put("meta_example_string", name)
	session.Metadata.Put(
		"meta_example_text",
		randomFrom([]string{"abc", "def", "ghi", "abcd", "abcdef", "abcgh"}),
	)
	session.Metadata.Put("meta_example_long", rand.Int63())
	session.Metadata.Put("meta_example_double", rand.Float64())
	session.Metadata.Put("meta_example_boolean", rand.Intn(2) == 1)
	session.Metadata.Put("meta_example_bytes", []byte(name))

	return session
}

func randomMessage(sessionID string) *model.Message {
	message := model.NewMessage(sessionID, uuid.NewString())
	message.SetCreateTime(rand.Int63n(100))

	name := faker.Name()

	content := strings.Join(
		randomList(
			[]string{
				"abc", "def", "ghi", "abcd", "adef",
				"abcgh", "apple", "banana", "cherry",
			},
			5,
		),
		" ",
	)

	message.SetContent(content)

	message.Metadata.Put("meta_example_string", name)
	message.Metadata.Put(
		"meta_example_text",
		randomFrom([]string{"abc", "def", "ghi", "abcd", "abcdef", "abcgh"}),
	)
	message.Metadata.Put("meta_example_long", rand.Int63())
	message.Metadata.Put("meta_example_double", rand.Float64())
	message.Metadata.Put("meta_example_boolean", rand.Intn(2) == 1)
	message.Metadata.Put("meta_example_bytes", []byte(name))

	return message
}

func randomFrom[T any](list []T) T {
	return list[rand.Intn(len(list))]
}

func randomList[T any](list []T, n int) []T {
	result := make([]T, n)
	for i := 0; i < n; i++ {
		result[i] = list[rand.Intn(len(list))]
	}
	return result
}
