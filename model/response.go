package model

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type Response[T any] struct {
	// Total total amount of search results
	Total int64 `json:"total,omitempty"`
	// Hits search results
	Hits []T `json:"hits,omitempty"`
	// NextStartPrimaryKey indicates the starting position for the next page.
	NextStartPrimaryKey *tablestore.PrimaryKey `json:"next_start_primary_key,omitempty"`
	// NextToken indicates the starting position for the next page (for search)
	NextToken []byte `json:"next_token,omitempty"`
}

