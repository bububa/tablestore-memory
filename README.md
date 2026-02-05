# Alibaba Cloud TableStore Memory for AI Agents

A memory store implementation for AI agents using Alibaba Cloud TableStore as the backend. This library provides session and message persistence capabilities optimized for AI agent applications.

[![Go Reference](https://pkg.go.dev/badge/github.com/bububa/tablestore-memory.svg)](https://pkg.go.dev/github.com/bububa/tablestore-memory)
[![Go](https://github.com/bububa/tablestore-memory/actions/workflows/go.yml/badge.svg)](https://github.com/bububa/tablestore-memory/actions/workflows/go.yml)
[![goreleaser](https://github.com/bububa/tablestore-memory/actions/workflows/goreleaser.yml/badge.svg)](https://github.com/bububa/tablestore-memory/actions/workflows/goreleaser.yml)
[![GitHub go.mod Go version of a Go module](https://img.shields.io/github/go-mod/go-version/bububa/tablestore-memory.svg)](https://github.com/bububa/tablestore-memory)
[![GoReportCard](https://goreportcard.com/badge/github.com/bububa/tablestore-memory)](https://goreportcard.com/report/github.com/bububa/tablestore-memory)
[![GitHub license](https://img.shields.io/github/license/bububa/tablestore-memory.svg)](https://github.com/bububa/tablestore-memory/blob/master/LICENSE)
[![GitHub release](https://img.shields.io/github/release/bububa/tablestore-memory.svg)](https://GitHub.com/bububa/tablestore-memory/releases/)

## Features

- **Session Management**: Store and retrieve AI agent sessions with metadata
- **Message Persistence**: Store conversation messages with timestamps
- **Scalable Storage**: Built on Alibaba Cloud TableStore for high-performance and scalability
- **Flexible Metadata**: Rich metadata support for session customization
- **Pagination Support**: Efficient pagination for large datasets
- **Microsecond Precision**: Timestamps with microsecond precision for accurate ordering

## Installation

```bash
go get github.com/bububa/tablestore-memory
```

## Quick Start

```go
package main

import (
	"log"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/bububa/tablestore-memory/tablestore"
	"github.com/bububa/tablestore-memory/model"
)

func main() {
	// Initialize TableStore client
	client := tablestore.NewClientWithConfig(
		"<endpoint>",
		"<instance-name>", 
		"<access-key-id>",
		"<access-key-secret>",
		tablestore.NewDefaultTableStoreConfig(),
	)

	// Create memory store instance
	store := tablestore.NewMemoryStore(client)

	// Initialize tables (only needed once)
	if err := store.InitTable(); err != nil {
		log.Fatal(err)
	}

	// Create a session
	session := model.NewSession("user-123", "session-456")
	session.SetMetadata(model.NewMetadata().Put("topic", "customer_support"))

	// Save the session
	if err := store.PutSession(session); err != nil {
		log.Fatal(err)
	}

	// Retrieve the session
	retrievedSession := model.NewSession("user-123", "session-456")
	if err := store.GetSession(retrievedSession); err != nil {
		log.Fatal(err)
	}

	log.Printf("Retrieved session: %+v", retrievedSession)
}
```

## Configuration

The library supports various configuration options for TableStore tables:

- **Session Table**: Stores session information with user ID, session ID, update time, and metadata
- **Message Table**: Stores conversation messages with session ID, message ID, create time, and content
- **Custom Table Names**: Configurable table names to avoid conflicts

## API Overview

### Session Operations
- `PutSession()` - Insert or overwrite a session
- `UpdateSession()` - Update an existing session
- `GetSession()` - Retrieve a session
- `DeleteSession()` - Delete a session
- `ListSessions()` - List sessions for a user
- `ListAllSessions()` - List all sessions

### Message Operations
- `PutMessage()` - Insert or overwrite a message
- `UpdateMessage()` - Update an existing message
- `GetMessage()` - Retrieve a message
- `DeleteMessage()` - Delete a message
- `ListMessages()` - List messages for a session
- `ListAllMessages()` - List all messages

### Advanced Operations
- `ListRecentSessionsPaginated()` - Paginated listing of recent sessions
- `ListMessagesWithFilter()` - Filtered message listing
- `ListMessagesPaginated()` - Paginated message listing

## Session Model

The Session model includes:
- `UserID` - Unique identifier for the user
- `SessionID` - Unique identifier for the session
- `UpdateTime` - Last update time in microseconds
- `Metadata` - Flexible metadata map with type-safe accessors

## Message Model

The Message model includes:
- `SessionID` - Associated session identifier
- `MessageID` - Unique message identifier
- `CreateTime` - Creation time in microseconds
- `Role` - Message role (user, assistant, system)
- `Content` - Message content
- `Metadata` - Flexible metadata map

## Error Handling

All operations return Go error types. The library uses standard error wrapping patterns:

```go
if err := store.PutSession(session); err != nil {
    return fmt.Errorf("failed to put session: %w", err)
}
```

## Testing

Run all tests:
```bash
go test ./...
```

Run tests with coverage:
```bash
go test -cover ./...
```

Run specific package tests:
```bash
go test ./model ./tablestore
```

## Development

This project follows Go best practices and includes:
- Comprehensive unit tests
- Standard Go error handling
- Clean interface design
- Proper resource management
- Consistent naming conventions

For development guidelines, see [AGENTS.md](AGENTS.md).

## License

MIT License - see the [LICENSE](LICENSE) file for details.
