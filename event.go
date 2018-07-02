package commander

import (
	"encoding/json"

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
		if header.Key == ActionHeader {
			event.Action = string(header.Value)
		}

		if header.Key == ParentHeader {
			parent, err := uuid.FromBytes(header.Value)

			if err != nil {
				return err
			}

			event.Parent = parent
		}

		if header.Key == KeyHeader {
			key, err := uuid.FromBytes(header.Value)

			if err != nil {
				return err
			}

			event.Key = key
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
