# AGENTS.MD - TableStore Memory Development Guidelines

## Build Commands

### Building the Project
```bash
# Build the entire project
go build ./...

# Build specific packages
go build ./tablestore
go build ./client
go build ./protocol
go build ./model

# Build with specific flags
go build -v ./...
```

### Testing Commands
```bash
# Run all tests
go test ./...

# Run tests for a specific package
go test ./model
go test ./protocol
go test ./tablestore
go test ./client

# Run a specific test (replace TestFunctionName with actual test name)
go test -run TestFunctionName ./package/path

# Run tests with verbose output
go test -v ./...

# Run tests with coverage
go test -cover ./...
go test -coverprofile=coverage.out ./...

# Run tests with race detector
go test -race ./...

# Run specific test file
go test ./model/session_test.go

# Run tests in a specific directory
go test ./tablestore/test/

# Run a specific test function in a package
go test -run ^TestSession_Simple$ ./model -v

# Skip integration tests that require external services
go test -short ./...
```

### Linting Commands
```bash
# Standard Go tools
go fmt ./...
go vet ./...
go mod tidy

# Install and run golangci-lint (recommended)
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
golangci-lint run

# Check for issues
gofmt -l .
goimports -l .
```

## Code Style Guidelines

### Imports Organization
- Group imports with blank lines between standard library, third-party, and project-specific imports
- Use full import paths for project-specific packages
- Example:
```go
import (
    "fmt"
    "os"
    "time"

    "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
    "github.com/spf13/cast"

    "github.com/bububa/tablestore-memory/model"
    "github.com/bububa/tablestore-memory/protocol"
)
```

### Naming Conventions
- Use camelCase for exported functions, types, and variables
- Exported functions should have clear, descriptive names
- Use PascalCase for types
- Use snake_case for private functions when appropriate
- Prefix interface names with "I" or use "er" suffix (e.g., `MemoryStore`)
- Constructor functions typically start with New (e.g., `NewSession`, `NewMemoryStore`)

### Type Definitions
- Define structs with clear field names
- Use pointer receivers for methods that modify the receiver
- Use value receivers for methods that don't modify the receiver and for small structs
- Always check for nil pointers before dereferencing
- Implement Clone methods with copy semantics as seen in `Session.Clone()`

### Error Handling
- Always handle errors explicitly, don't ignore them
- Return errors from functions that can fail
- Use error wrapping when appropriate: `fmt.Errorf("context: %w", err)`
- Use sentinel errors defined in the package when appropriate
- Don't log errors and return them together unless necessary
- Common error wrapping pattern used throughout: `fmt.Errorf("context: %w", err)`

### Commenting Standards
- Document all exported functions, types, and packages
- Use clear, concise comments that explain the "why" not just the "what"
- Add package-level documentation in doc.go files
- Use sentence case for comments and end with periods
- Comment complex algorithms or business logic thoroughly
- Use section markers like `// --------------------` to group related functionality

### Data Modeling Patterns
- Use Metadata maps for flexible attribute storage
- Implement Copy methods for deep copying metadata structures
- Use type-safe getters and setters for metadata access
- Follow consistent field naming conventions for primary keys and attributes

### Interface Design
- Follow the principle of small, focused interfaces
- Name interfaces based on their behavior (e.g., "Reader", "Writer", "Closer")
- Use interface{} only when truly generic behavior is needed
- Prefer concrete types over interface{} when possible
- Implement interfaces with explicit assertions like `var _ protocol.MemoryStore = (*MemoryStore)(nil)`
- Always validate input parameters and check for nil pointers before dereferencing

### Struct and Method Patterns
- Initialize structs with explicit field names for readability
- Use constructor functions for complex initialization (NewXxx pattern)
- Use functional options pattern for configuration when needed (e.g., `WithSessionTableName`, `WithMessageTableName`)
- Implement Clone methods for structs that need copying behavior
- Use fluent setters for builder-style APIs (return the struct pointer)
- Follow consistent patterns like `SetXxx()` methods that return the struct pointer for chaining
- Use `RefreshXxx()` pattern for updating fields to current values (e.g., `RefreshUpdateTime()`)

### Testing Guidelines
- Place test files in the same package as the code being tested
- Name test functions with "Test" prefix and descriptive names
- Use table-driven tests for multiple test cases
- Include benchmarks for performance-critical functions
- Test error cases as well as successful cases
- Use subtests for organizing related test cases
- Use `t.Fatalf()` for assertion failures that should stop the test
- Use `t.Errorf()` for assertion failures that should continue the test
- Test both simple and comprehensive scenarios (e.g., `TestSession_Simple`)
- Utilize helper functions and test utilities specific to this project

### Performance Considerations
- Use efficient data structures appropriate for the use case
- Minimize memory allocations in hot paths
- Reuse buffers when appropriate
- Consider concurrency patterns for I/O operations
- Profile code when performance is critical
- Pay attention to microsecond precision timing as used in this project (e.g., `CurrentTimeMicroseconds()`)
- Use appropriate batch sizes for database operations to optimize performance

### Security Practices
- Validate all input parameters
- Sanitize data before storing or transmitting
- Never log sensitive information
- Use secure random number generation for IDs and secrets
- Apply the principle of least privilege

### Documentation Requirements
- All public functions must have godoc comments
- Include examples in godoc when beneficial
- Document expected error types in function comments
- Update documentation when changing public interfaces
- Use consistent terminology throughout the codebase

### Dependency Management
- Keep dependencies minimal and up-to-date
- Use go mod tidy regularly to clean up unused dependencies
- Pin major versions of critical dependencies
- Document why non-obvious dependencies are needed
- This project specifically uses github.com/aliyun/aliyun-tablestore-go-sdk for TableStore operations
- Additional dependencies include github.com/spf13/cast for type casting, github.com/google/uuid for UUID generation, and github.com/go-faker/faker/v4 for test data

### Git Workflow
- Use descriptive commit messages following conventional commits
- Keep commits focused on a single logical change
- Use feature branches for new development
- Run all tests before pushing changes
- Update go.mod and go.sum when adding/removing dependencies

### Common Patterns and Utilities
- Use `CurrentTimeMicroseconds()` for timestamp generation with microsecond precision
- Return channels for streaming data operations (e.g., `<-chan model.Session`)
- Use response wrappers like `Response[T]` for paginated results
- Handle pagination with `nextStartPrimaryKey` for efficient data retrieval