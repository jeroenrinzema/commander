package commander

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

// Command contains the information of a consumed command.
type Command struct {
	Key     uuid.UUID         `json:"key,omitempty"`
	Headers map[string]string `json:"headers"`
	ID      uuid.UUID         `json:"id"`
	Action  string            `json:"action"`
	Data    json.RawMessage   `json:"data"`
}

// NewEvent creates a acknowledged event as a response to this command.
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

// NewErrorEvent creates a error event as a response to this command.
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

// Populate populates the command struct with the given kafka message
func (command *Command) Populate(message *kafka.Message) error {
	for _, header := range message.Headers {
		key := string(header.Key)
		value := string(header.Value)

		switch key {
		case ActionHeader:
			command.Action = value
		case IDHeader:
			id, err := uuid.FromString(value)

			if err != nil {
				return err
			}

			command.ID = id
		}

		command.Headers[key] = value
	}

	id, err := uuid.FromString(string(message.Key))

	if err != nil {
		return err
	}

	command.Key = id
	command.Data = message.Value

	return nil
}
