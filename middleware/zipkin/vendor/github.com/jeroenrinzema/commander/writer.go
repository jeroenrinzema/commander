package commander

import (
	"context"
	"errors"
	"time"

	"github.com/gofrs/uuid"
)

// NewResponseWriter initializes a new response writer for the given value
func NewResponseWriter(group *Group, value interface{}) ResponseWriter {
	writer := &writer{
		Group: group,
	}

	if command, ok := value.(Command); ok {
		writer.Command = &command
	}

	return writer
}

var (
	// ErrDefaultRetry represents the default retry error message
	ErrDefaultRetry = errors.New("message marked to be retried")
)

// ResponseWriter writes events or commands back to the assigned group.
type ResponseWriter interface {
	// ProduceEvent creates and produces a new event to the assigned group.
	ProduceEvent(action string, version int8, key uuid.UUID, data []byte) (Event, error)

	// ProduceError produces a new error event
	ProduceError(action string, status StatusCode, err error) (Event, error)

	// ProduceCommand produces a new command
	ProduceCommand(action string, version int8, key uuid.UUID, data []byte) (Command, error)

	// Retry marks the message to be retried. An error could be given to be passed to the dialect
	Retry(err error)

	// ShouldRetry returns if the writer has marked the message to be retried
	ShouldRetry() error
}

// Writer is a struct representing the ResponseWriter interface
type writer struct {
	Group   *Group
	Command *Command
	retry   error
}

func (writer *writer) ProduceError(action string, status StatusCode, err error) (Event, error) {
	var event Event

	if status == 0 {
		status = StatusInternalServerError
	}

	if writer.Command != nil {
		event = writer.Command.NewError(action, status, err)
	} else {
		event = NewEvent(action, 0, uuid.Nil, uuid.Nil, nil)
		event.Status = StatusInternalServerError

		if err != nil {
			event.Meta = err.Error()
		}
	}

	err = writer.Group.ProduceEvent(event)
	return event, err
}

func (writer *writer) ProduceEvent(action string, version int8, key uuid.UUID, data []byte) (Event, error) {
	var parent uuid.UUID
	var timestamp time.Time
	var ctx context.Context

	if writer.Command != nil {
		parent = writer.Command.ID
		timestamp = writer.Command.Timestamp
		ctx = writer.Command.Ctx
	}

	if key == uuid.Nil && writer.Command != nil {
		key = writer.Command.Key
	}

	event := NewEvent(action, version, parent, key, data)
	event.CommandTimestamp = timestamp
	event.Ctx = ctx

	err := writer.Group.ProduceEvent(event)
	return event, err
}

func (writer *writer) ProduceCommand(action string, version int8, key uuid.UUID, data []byte) (Command, error) {
	command := NewCommand(action, version, key, data)
	err := writer.Group.ProduceCommand(command)

	return command, err
}

func (writer *writer) Retry(err error) {
	if err != nil {
		err = ErrDefaultRetry
	}

	writer.retry = err
}

func (writer *writer) ShouldRetry() error {
	return writer.retry
}
