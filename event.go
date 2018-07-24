package commander

import (
	"encoding/json"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

// Event is produced as the result from a command
type Event struct {
	Parent       uuid.UUID       `json:"parent"`
	ID           uuid.UUID       `json:"id"`
	Action       string          `json:"action"`
	Data         json.RawMessage `json:"data"`
	Key          uuid.UUID       `json:"key"`
	Acknowledged bool            `json:"acknowledged"`
}

// Populate the event with the data from the given kafka message
func (event *Event) Populate(message *kafka.Message) error {
	for _, header := range message.Headers {
		switch header.Key {
		case ActionHeader:
			event.Action = string(header.Value)
		case ParentHeader:
			parent, err := uuid.FromBytes(header.Value)

			if err != nil {
				return err
			}

			event.Parent = parent
		case KeyHeader:
			key, err := uuid.FromBytes(header.Value)

			if err != nil {
				return err
			}

			event.Key = key
		case KeyAcknowledged:
			acknowledged, err := strconv.ParseBool(string(header.Value))

			if err != nil {
				return err
			}

			event.Acknowledged = acknowledged
		}
	}

	id, err := uuid.FromBytes(message.Key)

	if err != nil {
		return err
	}

	event.ID = id
	event.Data = message.Value

	return nil
}
