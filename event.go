package commander

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
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
	Origin       string            `json:"-"`
}

// Populate the event with the data from the given kafka message
func (event *Event) Populate(message *kafka.Message) error {
	event.Headers = make(map[string]string)

headers:
	for _, header := range message.Headers {
		switch string(header.Key) {
		case ActionHeader:
			event.Action = string(header.Value)
			continue headers
		case ParentHeader:
			parent, err := uuid.FromString(string(header.Value))

			if err != nil {
				return err
			}

			event.Parent = parent
			continue headers
		case IDHeader:
			id, err := uuid.FromString(string(header.Value))

			if err != nil {
				return err
			}

			event.ID = id
			continue headers
		case AcknowledgedHeader:
			acknowledged, err := strconv.ParseBool(string(header.Value))

			if err != nil {
				return err
			}

			event.Acknowledged = acknowledged
			continue headers
		case VersionHeader:
			version, err := strconv.ParseInt(string(header.Value), 10, 0)

			if err != nil {
				return err
			}

			event.Version = int(version)
			continue headers
		}

		event.Headers[string(header.Key)] = string(header.Value)
	}

	id, err := uuid.FromString(string(message.Key))

	if err != nil {
		return err
	}

	if len(event.Action) == 0 {
		return errors.New("No event action is set")
	}

	event.Key = id
	event.Data = message.Value
	event.Origin = *message.TopicPartition.Topic

	return nil
}
