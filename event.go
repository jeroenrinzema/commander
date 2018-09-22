package commander

import (
	"encoding/json"

	uuid "github.com/satori/go.uuid"
)

// Event contains the information of a consumed event.
// A event is produced as the result of a command.
type Event struct {
	Parent       uuid.UUID         `json:"parent"`
	Headers      map[string]string `json:"headers"`
	ID           uuid.UUID         `json:"id"`
	Action       string            `json:"action"`
	Data         json.RawMessage   `json:"data"`
	Key          uuid.UUID         `json:"key"`
	Acknowledged bool              `json:"acknowledged"`
	Version      int               `json:"version"`
}
