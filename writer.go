package commander

import (
	"errors"

	uuid "github.com/satori/go.uuid"
)

// ResponseWriter writes events or commands back to the assigned group.
type ResponseWriter interface {
	// ProduceEvent creates and produces a new event to the assigned group.
	ProduceEvent(action string, version int, key uuid.UUID, data []byte) (*Event, error)

	// ProduceError produces a new error event
	ProduceError(action string, data []byte) (*Event, error)
}

// Writer is a struct representing the ResponseWriter interface
type writer struct {
	Group   *Group
	command *Command
}

func (writer *writer) ProduceError(action string, data []byte) (*Event, error) {
	if writer.command == nil {
		return nil, errors.New("no command was set")
	}

	event := writer.command.NewErrorEvent(action, data)
	err := writer.Group.ProduceEvent(event)

	return event, err
}

func (writer *writer) ProduceEvent(action string, version int, key uuid.UUID, data []byte) (*Event, error) {
	if writer.command == nil {
		return nil, errors.New("no command was set")
	}

	event := writer.Group.NewEvent(action, version, writer.command.ID, key, data)
	err := writer.Group.ProduceEvent(event)

	return event, err
}
