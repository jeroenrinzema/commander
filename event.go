package commander

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/gofrs/uuid"
)

// StatusCode represents an message status code.
// The status codes are base on the HTTP status code specifications.
type StatusCode int16

// Status codes that represents the status of a event
const (
	StatusOK                  StatusCode = 200
	StatusBadRequest          StatusCode = 400
	StatusUnauthorized        StatusCode = 401
	StatusForbidden           StatusCode = 403
	StatusNotFound            StatusCode = 404
	StatusConflict            StatusCode = 409
	StatusImATeapot           StatusCode = 418
	StatusInternalServerError StatusCode = 500
)

// NewEvent constructs a new event
func NewEvent(action string, version int8, parent uuid.UUID, key uuid.UUID, data []byte) Event {
	id, err := uuid.NewV4()
	if err != nil {
		Logger.Println("Unable to generate a new uuid!")
		panic(err)
	}

	// Fix: unexpected end of JSON input
	if len(data) == 0 {
		data = []byte("null")
	}

	event := Event{
		Parent:  parent,
		ID:      id,
		Headers: make(map[string]string),
		Action:  action,
		Data:    data,
		Key:     key,
		Status:  StatusOK,
		Version: version,
		Ctx:     context.Background(),
	}

	return event
}

// Event contains the information of a consumed event.
// A event is produced as the result of a command.
type Event struct {
	Parent           uuid.UUID         `json:"parent"`            // Event command parent id
	Headers          map[string]string `json:"headers"`           // Additional event headers
	ID               uuid.UUID         `json:"id"`                // Unique event id
	Action           string            `json:"action"`            // Event representing action
	Data             []byte            `json:"data"`              // Passed event data as bytes
	Key              uuid.UUID         `json:"key"`               // Event partition key
	Status           StatusCode        `json:"status"`            // Event status code (commander.Status*)
	Version          int8              `json:"version"`           // Event data schema version
	Origin           Topic             `json:"-"`                 // Event topic origin
	Meta             string            `json:"meta"`              // Additional event meta message
	CommandTimestamp time.Time         `json:"command_timestamp"` // Timestamp of parent command append
	Timestamp        time.Time         `json:"timestamp"`         // Timestamp of event append
	Ctx              context.Context   `json:"-"`
}

// Populate the event with the data from the given message.
// If a error occures during the parsing of the message is the error silently thrown and returned.
// When a error is thrown is will also a log be logged if an output writer is given to the commander logger.
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
				continue
			}

			event.Parent = parent
			continue headers
		case IDHeader:
			id, err := uuid.FromString(str)

			if err != nil {
				throw = err
				continue
			}

			event.ID = id
			continue headers
		case StatusHeader:
			status, err := strconv.ParseInt(str, 10, 16)

			if err != nil {
				throw = err
				continue
			}

			event.Status = StatusCode(int16(status))
			continue headers
		case VersionHeader:
			version, err := strconv.ParseInt(str, 10, 8)
			if err != nil {
				throw = err
				continue
			}

			event.Version = int8(version)
			continue headers
		case MetaHeader:
			event.Meta = str
			continue headers
		case CommandTimestampHeader:
			unix, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				throw = err
				continue
			}

			time := time.Unix(unix, 0)
			event.CommandTimestamp = time
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
	event.Timestamp = message.Timestamp

	if throw != nil {
		Logger.Println("A error was thrown when populating the command message:", throw)
	}

	return throw
}

// Message constructs a new commander message for the given event
func (event *Event) Message(topic Topic) *Message {
	headers := make(map[string]string)
	for key, value := range event.Headers {
		headers[key] = value
	}

	headers[ActionHeader] = event.Action
	headers[ParentHeader] = event.Parent.String()
	headers[IDHeader] = event.ID.String()
	headers[StatusHeader] = strconv.Itoa(int(event.Status))
	headers[VersionHeader] = strconv.Itoa(int(event.Version))
	headers[MetaHeader] = event.Meta
	headers[CommandTimestampHeader] = strconv.Itoa(int(event.CommandTimestamp.Unix()))

	message := &Message{
		Headers: headers,
		Key:     []byte(event.Key.String()),
		Value:   event.Data,
		Topic:   topic,
		Ctx:     context.Background(),
	}

	return message
}
