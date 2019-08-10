package commander

import (
	"context"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander/types"
)

// NewCommand constructs a new command
func NewCommand(action string, version types.Version, key types.Key, data []byte) Command {
	id := uuid.Must(uuid.NewV4()).String()
	if key == nil {
		key = types.Key([]byte(id))
	}

	return Command{
		ID:        id,
		Key:       key,
		Action:    action,
		Version:   version,
		Data:      data,
		Timestamp: time.Now(),
		Ctx:       context.Background(),
	}
}

// NewCommandFromMessage constructs a new Command and fill the command with the data from the given message.
func NewCommandFromMessage(message *types.Message) Command {
	command := Command{}

	command.ID = message.ID
	command.Key = message.Key
	command.Action = message.Action
	command.Data = message.Data
	command.Version = message.Version
	command.Origin = message.Topic
	command.Timestamp = message.Timestamp
	command.Ctx = message.Ctx

	return command
}

// Command contains the information of a consumed command.
type Command struct {
	ID        string          `json:"id"`        // Unique command id
	Key       types.Key       `json:"key"`       // Command partition key
	Action    string          `json:"action"`    // Command representing action
	Data      []byte          `json:"data"`      // Passed command data as bytes
	Version   types.Version   `json:"version"`   // Command data schema version
	Origin    types.Topic     `json:"-"`         // Command topic origin
	EOS       types.EOS       `json:"eos"`       // EOS (end of stream) indicator
	Timestamp time.Time       `json:"timestamp"` // Timestamp of command append
	Ctx       context.Context `json:"-"`
}

// NewCommand creates a new "child" command that is linked to a stream of commands.
func (command *Command) NewCommand(action string, version types.Version, data []byte) Command {
	return Command{
		ID:        command.ID,
		Key:       command.Key,
		Action:    action,
		Version:   version,
		Data:      data,
		Origin:    command.Origin,
		Timestamp: time.Now(),
		Ctx:       context.Background(),
	}
}

// NewEvent creates a new acknowledged event as a response to this command.
func (command *Command) NewEvent(action string, version types.Version, data []byte) Event {
	event := NewEvent(action, version, types.ParentID(command.ID), command.Key, data)
	event.ParentTimestamp = types.ParentTimestamp(command.Timestamp)
	event.Ctx = command.Ctx

	return event
}

// NewError creates a error event as a response to this command.
func (command *Command) NewError(action string, status types.StatusCode, err error) Event {
	event := NewEvent(action, 0, types.ParentID(command.ID), command.Key, nil)
	event.Status = status
	event.ParentTimestamp = types.ParentTimestamp(command.Timestamp)
	event.Ctx = command.Ctx

	// TODO: pass err message

	return event
}

// Message constructs a new commander message for the given command
func (command *Command) Message(topic Topic) *Message {
	message := &Message{
		ID:        command.ID,
		Key:       command.Key,
		Action:    command.Action,
		Data:      command.Data,
		Version:   command.Version,
		Topic:     topic,
		Timestamp: time.Now(),
		EOS:       command.EOS,
		Ctx:       command.Ctx,
	}

	return message
}
