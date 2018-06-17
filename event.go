package commander

import (
	"encoding/json"

	uuid "github.com/satori/go.uuid"
)

const (
	// CreateOperation ...
	CreateOperation = "create"
	// UpdateOperation ...
	UpdateOperation = "update"
	// DeleteOperation ...
	DeleteOperation = "delete"
)

// Event ...
type Event struct {
	Parent    uuid.UUID       `json:"parent"`
	ID        uuid.UUID       `json:"id"`
	Action    string          `json:"action"`
	Data      json.RawMessage `json:"data"`
	Operation string          `json:"operation"`
	Key       uuid.UUID       `json:"key"`
	commander *Commander
}

// Produce the created event
func (e *Event) Produce() {
	e.commander.ProduceEvent(e)
}
