package commander

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/gofrs/uuid"
)

// Status codes that represents the status of a event
const (
	StatusOK                  = 200
	StatusBadRequest          = 400
	StatusInternalServerError = 500
)

// NewEvent constructs a new event
func NewEvent(action string, version int8, parent uuid.UUID, key uuid.UUID, data []byte) *Event {
	id, err := uuid.NewV4()
	if err != nil {
		Logger.Println("Unable to generate a new uuid!")
		panic(err)
	}

	// Fix: unexpected end of JSON input
	if len(data) == 0 {
		data = []byte("null")
	}

	event := &Event{
		Parent:  parent,
		ID:      id,
		Headers: make(map[string]string),
		Action:  action,
		Data:    data,
		Key:     key,
		Status:  StatusOK,
		Version: version,
	}

	return event
}

// Event contains the information of a consumed event.
// A event is produced as the result of a command.
type Event struct {
	Parent  uuid.UUID         `json:"parent"`
	Headers map[string]string `json:"headers"`
	ID      uuid.UUID         `json:"id"`
	Action  string            `json:"action"`
	Data    json.RawMessage   `json:"data"`
	Key     uuid.UUID         `json:"key"`
	Status  int16             `json:"status"`
	Version int8              `json:"version"`
	Origin  Topic             `json:"-"`
}

// Populate the event with the data from the given message
func (event *Event) Populate(message *Message) error {
	Logger.Println("Populating a event from a message")

	event.Headers = make(map[string]string)
	var throw error

headers:
	for key, value := range message.Headers {
		str := string(value)

		switch key {
		case ActionHeader:
			event.Action = str
			continue headers
		case ParentHeader:
			parent, err := uuid.FromString(str)

			if err != nil {
				throw = err
			}

			event.Parent = parent
			continue headers
		case IDHeader:
			id, err := uuid.FromString(str)

			if err != nil {
				throw = err
			}

			event.ID = id
			continue headers
		case StatusHeader:
			status, err := strconv.ParseInt(str, 10, 16)

			if err != nil {
				throw = err
			}

			event.Status = int16(status)
			continue headers
		case VersionHeader:
			version, err := strconv.ParseInt(str, 10, 8)
			if err != nil {
				throw = err
			}

			event.Version = int8(version)
			continue headers
		}

		event.Headers[key] = str
	}

	id, err := uuid.FromString(string(message.Key))

	if err != nil {
		throw = err
	}

	if len(event.Action) == 0 {
		throw = errors.New("No event action is set")
	}

	event.Key = id
	event.Data = message.Value
	event.Origin = message.Topic

	if throw != nil {
		Logger.Println("A error was thrown when populating the command message:", throw)
	}

	return throw
}
