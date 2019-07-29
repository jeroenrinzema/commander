package commander

import (
	"context"
	"errors"

	"github.com/jeroenrinzema/commander/types"
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
	// The produced event is one of many events in the event stream.
	ProduceEvent(action string, version int8, key []byte, data []byte) (Event, error)

	// ProduceEvent creates and produces a new event to the assigned group.
	// The produced event is one of many events in the event stream.
	// The produced event is marked as EOS (end of stream).
	ProduceEventEOS(action string, version int8, key []byte, data []byte) (Event, error)

	// ProduceError produces a new error event to the assigned group.
	// The produced error is one of many events in the event stream.
	// The produced event is marked as EOS (end of stream).
	ProduceError(action string, status types.StatusCode, err error) (Event, error)

	// ProduceError produces a new error event to the assigned group.
	// The produced error is one of many events in the event stream.
	ProduceErrorEOS(action string, status types.StatusCode, err error) (Event, error)

	// ProduceCommand produces a new command to the assigned group.
	// The produced comamnd is one of many commands in the command stream.
	// The produced event is marked as EOS (end of stream).
	ProduceCommand(action string, version int8, key []byte, data []byte) (Command, error)

	// ProduceCommand produces a new command to the assigned group.
	// The produced comamnd is one of many commands in the command stream.
	ProduceCommandEOS(action string, version int8, key []byte, data []byte) (Command, error)

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

func (writer *writer) NewErrorEvent(action string, status types.StatusCode, err error) Event {
	var event Event

	if status == 0 {
		status = StatusInternalServerError
	}

	if writer.Command != nil {
		event = writer.Command.NewError(action, status, err)
	} else {
		event = NewEvent(action, 0, "", nil, nil)
		event.Status = StatusInternalServerError
	}

	return event
}

func (writer *writer) NewEvent(action string, version int8, key []byte, data []byte) Event {
	var timestamp types.ParentTimestamp
	var parent types.ParentID

	if writer.Command != nil {
		timestamp = types.ParentTimestamp(writer.Command.Timestamp)
		parent = types.ParentID(writer.Command.ID)
	}

	if key == nil && writer.Command != nil {
		key = writer.Command.Key
	}

	event := NewEvent(action, types.Version(version), parent, types.Key(key), data)
	event.ParentTimestamp = timestamp
	event.Ctx = context.Background()

	if writer.Command != nil {
		event.Ctx = writer.Command.Ctx
	}

	return event
}

func (writer *writer) ProduceError(action string, status types.StatusCode, err error) (Event, error) {
	event := writer.NewErrorEvent(action, status, err)

	err = writer.Group.ProduceEvent(event)
	return event, err
}

func (writer *writer) ProduceErrorEOS(action string, status types.StatusCode, err error) (Event, error) {
	event := writer.NewErrorEvent(action, status, err)
	event.EOS = true

	err = writer.Group.ProduceEvent(event)
	return event, err
}

func (writer *writer) ProduceEvent(action string, version int8, key []byte, data []byte) (Event, error) {
	event := writer.NewEvent(action, version, key, data)

	err := writer.Group.ProduceEvent(event)
	return event, err
}

func (writer *writer) ProduceEventEOS(action string, version int8, key []byte, data []byte) (Event, error) {
	event := writer.NewEvent(action, version, key, data)
	event.EOS = true

	err := writer.Group.ProduceEvent(event)
	return event, err
}

func (writer *writer) ProduceCommand(action string, version int8, key []byte, data []byte) (Command, error) {
	command := NewCommand(action, types.Version(version), types.Key(key), data)

	err := writer.Group.ProduceCommand(command)
	return command, err
}

func (writer *writer) ProduceCommandEOS(action string, version int8, key []byte, data []byte) (Command, error) {
	command := NewCommand(action, types.Version(version), types.Key(key), data)
	command.EOS = true

	err := writer.Group.ProduceCommand(command)
	return command, err
}

func (writer *writer) Retry(err error) {
	if err == nil {
		err = ErrDefaultRetry
	}

	writer.retry = err
}

func (writer *writer) ShouldRetry() error {
	return writer.retry
}
