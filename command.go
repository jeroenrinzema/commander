package commander

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	uuid "github.com/satori/go.uuid"
)

// NewCommand create a new command with the given action and data.
// A unique ID is generated and set in order to trace the command.
func NewCommand(action string, data []byte) *Command {
	id := uuid.NewV4()

	command := Command{
		ID:     id,
		Action: action,
		Data:   data,
	}

	return &command
}

// Command a command contains a order for a data change
type Command struct {
	Key    uuid.UUID       `json:"key,omitempty"`
	ID     uuid.UUID       `json:"id"`
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data"`
}

// NewEvent create a new event as a respond to the consumed command
func (command *Command) NewEvent(action string, version int, key uuid.UUID, data []byte) *Event {
	id := uuid.NewV4()
	event := &Event{
		Parent:       command.ID,
		ID:           id,
		Action:       action,
		Data:         data,
		Key:          key,
		Acknowledged: true,
		Version:      version,
	}

	return event
}

// NewErrorEvent creates a new error event as a respond to the command
func (command *Command) NewErrorEvent(action string, data []byte) *Event {
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
func (command *Command) Populate(message *sarama.ConsumerMessage) error {
	for _, header := range message.Headers {
		switch string(header.Key) {
		case ActionHeader:
			command.Action = string(header.Value)
		case IDHeader:
			id, err := uuid.FromBytes(header.Value)

			if err != nil {
				return err
			}

			command.ID = id
		}
	}

	id, err := uuid.FromString(string(message.Key))

	if err != nil {
		return err
	}

	command.Key = id
	command.Data = message.Value

	return nil
}
