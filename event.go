package commander

import (
	"context"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander/metadata"
	"github.com/jeroenrinzema/commander/types"
)

// Status codes that represents the status of a event
const (
	StatusOK                  = types.StatusOK
	StatusBadRequest          = types.StatusBadRequest
	StatusUnauthorized        = types.StatusUnauthorized
	StatusForbidden           = types.StatusForbidden
	StatusNotFound            = types.StatusNotFound
	StatusConflict            = types.StatusConflict
	StatusImATeapot           = types.StatusImATeapot
	StatusInternalServerError = types.StatusInternalServerError
)

// NewEvent constructs a new event
func NewEvent(action string, version types.Version, parent types.ParentID, key types.Key, data []byte) Event {
	id := uuid.Must(uuid.NewV4()).String()
	if key == nil {
		key = types.Key([]byte(id))
	}

	event := Event{
		ID:      id,
		Parent:  parent,
		Action:  action,
		Data:    data,
		Key:     key,
		Status:  types.StatusOK,
		Version: version,
		Ctx:     context.Background(),
	}

	return event
}

// NewEventFromMessage constructs a new Event and fill the event with the data from the given message.
func NewEventFromMessage(message *Message) Event {
	event := Event{}

	parent, _ := metadata.ParentIDFromContext(message.Ctx)
	parentTimestamp, _ := metadata.ParentTimestampFromContext(message.Ctx)
	status, _ := metadata.StatusCodeFromContext(message.Ctx)

	event.ID = message.ID
	event.Action = message.Action
	event.Data = message.Data
	event.Key = message.Key
	event.Version = message.Version
	event.EOS = message.EOS
	event.Status = status
	event.Parent = parent
	event.ParentTimestamp = parentTimestamp
	event.Origin = message.Topic
	event.Timestamp = message.Timestamp
	event.Ctx = message.Ctx

	return event
}

// Event contains the information of a consumed event.
// A event is produced as the result of a command.
type Event struct {
	ID              string                `json:"id"`               // Unique event id
	Action          string                `json:"action"`           // Event representing action
	Data            []byte                `json:"data"`             // Passed event data as bytes
	Key             types.Key             `json:"key"`              // Event partition key
	Version         types.Version         `json:"version"`          // Event data schema version
	EOS             bool                  `json:"eos"`              // EOS (end of stream) indicator
	Status          types.StatusCode      `json:"status"`           // Event status code (commander.Status*)
	Origin          types.Topic           `json:"origin"`           // Event topic origin
	Parent          types.ParentID        `json:"parent"`           // Event command parent id
	ParentTimestamp types.ParentTimestamp `json:"parent_timestamp"` // Timestamp of parent command append
	Timestamp       time.Time             `json:"timestamp"`        // Timestamp of event append
	Ctx             context.Context       `json:"-"`
}

// Message constructs a new commander message for the given event
func (event *Event) Message(topic Topic) *Message {
	message := &Message{
		ID:        event.ID,
		Action:    event.Action,
		Data:      event.Data,
		Key:       event.Key,
		Version:   event.Version,
		EOS:       event.EOS,
		Topic:     topic,
		Timestamp: time.Now(),
		Ctx:       event.Ctx,
	}

	return message
}
