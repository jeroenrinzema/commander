package commander

import (
	"encoding/json"

	uuid "github.com/satori/go.uuid"
)

// Command contains the information of a consumed command.
type Command struct {
	Key    uuid.UUID       `json:"key,omitempty"`
	ID     uuid.UUID       `json:"id"`
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data"`
}
