package streams

import (
	"encoding/json"
)

type OperationType string

const (
	OpInsert OperationType = "INSERT"
	OpUpdate OperationType = "UPDATE"
	OpDelete OperationType = "DELETE"
)

// CDCEvent to parse the stream data from postgres
type CDCEvent struct {
	ID        string          `json:"id"`
	Table     string          `json:"table"`
	Schema    string          `json:"schema"`
	Op        OperationType   `json:"op"`
	Timestamp int64           `json:"timestamp"`
	Before    json.RawMessage `json:"before"`
	After     json.RawMessage `json:"after"`
}
