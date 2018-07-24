package commander

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

// Command a command contains a order for a data change
type Command struct {
	Key    uuid.UUID       `json:"key,omitempty"`
	ID     uuid.UUID       `json:"id"`
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data"`
}

// NewEvent create a new event as a respond to the command
func (command *Command) NewEvent(action string, key uuid.UUID, data []byte) *Event {
	id := uuid.NewV4()
	event := &Event{
		Parent:       command.ID,
		ID:           id,
		Action:       action,
		Data:         data,
		Key:          key,
		Acknowledged: true,
	}

	return event
}

// NewError creates a new error event as a respond to the command
func (command *Command) NewError(action string, data []byte) *Event {
	id := uuid.NewV4()
	key := uuid.Nil
	event := &Event{
		Parent:       command.ID,
		ID:           id,
		Action:       action,
		Data:         data,
		Key:          key,
		Acknowledged: false,
	}

	return event
}

// Populate populate the command with the data from a kafka message
func (command *Command) Populate(msg *kafka.Message) error {
	for _, header := range msg.Headers {
    switch header.Key {
    case ActionHeader:
      command.Action = string(header.Value)
      break
    }
	}

	id, err := uuid.FromBytes(msg.Key)

	if err != nil {
		return err
	}

	command.ID = id
	command.Data = msg.Value

	return nil
}
