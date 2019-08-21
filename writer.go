package commander

import (
	"log"

	"github.com/jeroenrinzema/commander/types"
)

// NewWriter initializes a new response writer for the given value
func NewWriter(group *Group, parent *Message) Writer {
	writer := &writer{
		group:  group,
		parent: parent,
	}

	return writer
}

// Writer handle implementation for a given group and message
type Writer interface {
	// Event creates and produces a new event to the assigned group.
	// The produced event is marked as EOS (end of stream).
	Event(action string, version int8, key []byte, data []byte) (*Message, error)

	// EventStream creates and produces a new event to the assigned group.
	// The produced event is one of many events in the event stream.
	EventStream(action string, version int8, key []byte, data []byte) (*Message, error)

	// EventEOS alias of Event
	EventEOS(action string, version int8, key []byte, data []byte) (*Message, error)

	// Error produces a new error event to the assigned group.
	// The produced error event is marked as EOS (end of stream).
	Error(action string, status types.StatusCode, err error) (*Message, error)

	// ErrorStream produces a new error event to the assigned group.
	// The produced error is one of many events in the event stream.
	ErrorStream(action string, status types.StatusCode, err error) (*Message, error)

	// ErrorEOS alias of Error
	ErrorEOS(action string, status types.StatusCode, err error) (*Message, error)

	// Command produces a new command to the assigned group.
	// The produced error event is marked as EOS (end of stream).
	Command(action string, version int8, key []byte, data []byte) (*Message, error)

	// CommandStream produces a new command to the assigned group.
	// The produced comamnd is one of many commands in the command stream.
	CommandStream(action string, version int8, key []byte, data []byte) (*Message, error)

	// CommandEOS alias of Command
	CommandEOS(action string, version int8, key []byte, data []byte) (*Message, error)
}

// Writer is a struct representing the ResponseWriter interface
type writer struct {
	group  *Group
	parent *Message
}

// NewMessage constructs a new message or a child of the parent.
func (writer *writer) NewMessage(action string, version int8, key []byte, data []byte) *Message {
	log.Println(writer.parent)
	if writer.parent != nil {
		return writer.parent.NewMessage(action, types.Version(version), types.Key(key), data)
	}

	return types.NewMessage(action, version, key, data)
}

func (writer *writer) Error(action string, status types.StatusCode, err error) (*Message, error) {
	return writer.ErrorEOS(action, status, err)
}

func (writer *writer) ErrorEOS(action string, status types.StatusCode, err error) (*Message, error) {
	if status == types.NullStatusCode {
		status = types.StatusInternalServerError
	}

	var payload []byte
	if err != nil {
		payload = []byte(err.Error())
	}

	message := writer.NewMessage(action, 0, nil, payload)
	message.Status = status
	message.EOS = true

	err = writer.group.ProduceEvent(message)
	return message, err
}

func (writer *writer) ErrorStream(action string, status types.StatusCode, err error) (*Message, error) {
	if status == types.NullStatusCode {
		status = types.StatusInternalServerError
	}

	var payload []byte
	if err != nil {
		payload = []byte(err.Error())
	}

	message := writer.NewMessage(action, 0, nil, payload)
	message.Status = status

	err = writer.group.ProduceEvent(message)
	return message, err
}

func (writer *writer) Event(action string, version int8, key []byte, data []byte) (*Message, error) {
	return writer.EventEOS(action, version, key, data)
}

func (writer *writer) EventEOS(action string, version int8, key []byte, data []byte) (*Message, error) {
	message := writer.NewMessage(action, version, key, data)
	message.EOS = true

	err := writer.group.ProduceEvent(message)
	return message, err
}

func (writer *writer) EventStream(action string, version int8, key []byte, data []byte) (*Message, error) {
	message := writer.NewMessage(action, version, key, data)

	err := writer.group.ProduceEvent(message)
	return message, err
}

func (writer *writer) Command(action string, version int8, key []byte, data []byte) (*Message, error) {
	return writer.CommandEOS(action, version, key, data)
}

func (writer *writer) CommandEOS(action string, version int8, key []byte, data []byte) (*Message, error) {
	message := writer.NewMessage(action, version, key, data)
	message.EOS = true

	err := writer.group.ProduceCommand(message)
	return message, err
}

func (writer *writer) CommandStream(action string, version int8, key []byte, data []byte) (*Message, error) {
	message := writer.NewMessage(action, version, key, data)

	err := writer.group.ProduceCommand(message)
	return message, err
}
