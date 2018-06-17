package commander

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

// Command ...
type Command struct {
	ID        uuid.UUID       `json:"id"`
	Action    string          `json:"action"`
	Data      json.RawMessage `json:"data"`
	commander *Commander
}

// NewEvent create a new command with the given action and data
func (c *Command) NewEvent(action string, operation string, key uuid.UUID, data []byte) Event {
	id := uuid.NewV4()

	event := Event{
		Parent:    c.ID,
		ID:        id,
		Action:    action,
		Data:      data,
		Operation: operation,
		Key:       key,
		commander: c.commander,
	}

	return event
}

// Populate ...
func (c *Command) Populate(msg *kafka.Message) error {
	for _, header := range msg.Headers {
		if header.Key == "action" {
			c.Action = string(header.Value)
		}
	}

	id, err := uuid.FromBytes(msg.Key)

	if err != nil {
		return err
	}

	c.ID = id
	c.Data = msg.Value

	return nil
}
