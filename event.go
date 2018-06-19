package commander

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

const (
	// CreateOperation event create operation
	CreateOperation = "create"
	// UpdateOperation event update operation
	UpdateOperation = "update"
	// DeleteOperation event delete operation
	DeleteOperation = "delete"
)

// Event a event sourcing event stored in the event topic
// a event contains a data change
type Event struct {
	Parent    uuid.UUID       `json:"parent"`
	ID        uuid.UUID       `json:"id"`
	Action    string          `json:"action"`
	Data      json.RawMessage `json:"data"`
	Operation string          `json:"operation"`
	Key       uuid.UUID       `json:"key"`
	commander *Commander
}

// Produce the created event
func (e *Event) Produce() {
	e.commander.ProduceEvent(e)
}

// Populate populate the event with the data from a kafka message
func (e *Event) Populate(msg *kafka.Message) error {
	for _, header := range msg.Headers {
		if header.Key == ActionHeader {
			e.Action = string(header.Value)
		}

		if header.Key == ParentHeader {
			parent, err := uuid.FromBytes(header.Value)

			if err != nil {
				return err
			}

			e.Parent = parent
		}

		if header.Key == OperationHeader {
			e.Operation = string(header.Value)
		}

		if header.Key == KeyHeader {
			key, err := uuid.FromBytes(header.Value)

			if err != nil {
				return err
			}

			e.Key = key
		}
	}

	id, err := uuid.FromBytes(msg.Key)

	if err != nil {
		return err
	}

	e.ID = id
	e.Data = msg.Value

	return nil
}
