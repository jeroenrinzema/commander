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
	Parent uuid.UUID   `json:"parent"`
	ID     uuid.UUID   `json:"id"`
	Action string      `json:"action"`
	Data   interface{} `json:"data"`
}
