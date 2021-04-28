package commander

import (
	"github.com/jeroenrinzema/commander/internal/metadata"
	"github.com/jeroenrinzema/commander/internal/types"
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
type Writer = types.Writer

// Writer is a struct representing the ResponseWriter interface
type writer struct {
	group  *Group
	parent *Message
}

// NewMessage constructs a new message or a child of the parent.
func (writer *writer) NewMessage(action string, version int8, key []byte, data []byte) *Message {
	if writer.parent != nil {
		return writer.parent.NewMessage(action, types.Version(version), metadata.Key(key), data)
	}

	return types.NewMessage(action, version, key, data)
}

func (writer *writer) Error(action string, status types.StatusCode, err error) (*Message, error) {
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
	message := writer.NewMessage(action, version, key, data)
	err := writer.group.ProduceEvent(message)
	return message, err
}

func (writer *writer) Command(action string, version int8, key []byte, data []byte) (*Message, error) {
	message := writer.NewMessage(action, version, key, data)

	err := writer.group.ProduceCommand(message)
	return message, err
}
