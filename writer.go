package commander

import (
	uuid "github.com/satori/go.uuid"
)

// NewResponseWriter initializes a new response writer for the given value
func NewResponseWriter(value interface{}) ResponseWriter {
	writer := &writer{}

	if command, ok := value.(*Command); ok {
		writer.Command = command
	}

	return writer
}

// ResponseWriter writes events or commands back to the assigned group.
type ResponseWriter interface {
	// ProduceEvent creates and produces a new event to the assigned group.
	ProduceEvent(action string, version int, key uuid.UUID, data []byte) (*Event, error)

	// ProduceError produces a new error event
	ProduceError(action string, data []byte) (*Event, error)

	// ProduceCommand produces a new command
	ProduceCommand(action string, key uuid.UUID, data []byte) (*Command, error)
}

// Writer is a struct representing the ResponseWriter interface
type writer struct {
	Group   *Group
	Command *Command
}

func (writer *writer) ProduceError(action string, data []byte) (*Event, error) {
	var event *Event

	if writer.Command != nil {
		event = writer.Command.NewError(action, data)
	}

	if event == nil {
		event = NewEvent(action, 0, uuid.Nil, uuid.Nil, data)
		event.Acknowledged = false
	}

	err := writer.Group.ProduceEvent(event)
	return event, err
}

func (writer *writer) ProduceEvent(action string, version int, key uuid.UUID, data []byte) (*Event, error) {
	parent := uuid.Nil

	if writer.Command != nil {
		parent = writer.Command.ID
	}

	if key == uuid.Nil && writer.Command != nil {
		key = writer.Command.Key
	}

	event := NewEvent(action, version, parent, key, data)
	err := writer.Group.ProduceEvent(event)

	return event, err
}

func (writer *writer) ProduceCommand(action string, key uuid.UUID, data []byte) (*Command, error) {
	command := NewCommand(action, key, data)
	err := writer.Group.ProduceCommand(command)

	return command, err
}
