package commander

import (
	uuid "github.com/satori/go.uuid"
)

// Command ...
type Command struct {
	ID     uuid.UUID   `json:"id"`
	Action string      `json:"action"`
	Data   interface{} `json:"data"`
}

// Event ...
type Event struct {
	Command
	Parent uuid.UUID `json:"parent"`
}
