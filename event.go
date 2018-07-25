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

var handlerMap map[string]func(event *Event, headerValue []byte) error{
	ActionHeader: HandleActionHeader,
	ParentHeader: HandleParentHeader,
	AcknowledgedHeader: HandleAcknowledgedHeader,
	KeyHeader: HandleKeyHeader,
}

func HandleActionHeader(event *Event, headerValue []byte) error {
	event.Action = string(headerValue)
	return nil
}

func HandleParentHeader(event *Event, headerValue []byte) error {
	parent, err := uuid.FromBytes(headerValue)
	if err != nil {
		return err
	}
	event.Parent = parent
	return nil
}

func HandleAcknowledgedHeader(event *Event, headerValue []byte) error {
	acknowledged, err := strconv.ParseBool(string(header.Value))
	if err != nil {
		return err
	}
	event.Acknowledged = acknowledged
	return nil
}

func HandleKeyHeader(event *Event, headerValue []byte) error {
	key, err := uuid.FromBytes(header.Value)
	if err != nil {
		return err
	}
	event.Key = key
	return nil
}

// Populate the event with the data from the given kafka message
func (event *Event) Populate(message *kafka.Message) error {
	for _, header := range message.Headers {
		if v, ok := handlerMap[header.Key]; ok {
			err := v()
			if err != nil {
				return err
			}
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
