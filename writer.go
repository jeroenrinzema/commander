package commander

import uuid "github.com/satori/go.uuid"

// ResponseWriter writes events or commands back to the assigned group.
type ResponseWriter interface {
	// ProduceEvent creates and produces a new event to the assigned group.
	ProduceEvent(action string, version int, parent uuid.UUID, key uuid.UUID, data []byte) (*Event, error)

	// AsyncCommand creates and produces a new command to the assigned group.
	AsyncCommand(action string, key uuid.UUID, data []byte) (*Command, error)

	// SyncCommand creates and produces a new command and awaits the responding event.
	// The timeout used is set in the group.
	SyncCommand(action string, key uuid.UUID, data []byte) (*Event, error)
}

// Writer is a struct representing the ResponseWriter interface
type writer struct {
	Group *Group
}

func (writer *writer) ProduceEvent(action string, version int, parent uuid.UUID, key uuid.UUID, data []byte) (*Event, error) {
	event := writer.Group.NewEvent(action, version, parent, key, data)
	err := writer.Group.ProduceEvent(event)

	return event, err
}

func (writer *writer) AsyncCommand(action string, key uuid.UUID, data []byte) (*Command, error) {
	command := writer.Group.NewCommand(action, key, data)
	err := writer.Group.AsyncCommand(command)

	return command, err
}

func (writer *writer) SyncCommand(action string, key uuid.UUID, data []byte) (*Event, error) {
	command := writer.Group.NewCommand(action, key, data)
	event, err := writer.Group.SyncCommand(command)

	return event, err
}
