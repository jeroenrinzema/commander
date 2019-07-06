package commander

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/gofrs/uuid"
)

// NewCommand constructs a new command
func NewCommand(action string, version int8, key []byte, data []byte) Command {
	id := uuid.Must(uuid.NewV4())
	if key == nil {
		key = id.Bytes()
	}

	return Command{
		Key:       key,
		Headers:   make(map[string]string),
		ID:        id,
		Action:    action,
		Version:   version,
		Data:      data,
		Timestamp: time.Now(),
		Ctx:       context.Background(),
	}
}

// Command contains the information of a consumed command.
type Command struct {
	Key       []byte            `json:"key"`       // Command partition key
	Headers   map[string]string `json:"headers"`   // Additional command headers
	ID        uuid.UUID         `json:"id"`        // Unique command id
	Action    string            `json:"action"`    // Command representing action
	Data      []byte            `json:"data"`      // Passed command data as bytes
	Version   int8              `json:"version"`   // Command data schema version
	Origin    Topic             `json:"-"`         // Command topic origin
	Offset    int               `json:"-"`         // Command message offset
	Partition int               `json:"-"`         // Command message partition
	EOS       bool              `json:"eos"`       // EOS (end of stream) indicator
	Timestamp time.Time         `json:"timestamp"` // Timestamp of command append
	Ctx       context.Context   `json:"-"`
}

// NewCommand creates a new "child" command that is linked to a stream of commands.
func (command *Command) NewCommand(action string, version int8, data []byte) Command {
	return Command{
		Key:       command.Key,
		Headers:   make(map[string]string),
		ID:        command.ID,
		Action:    action,
		Version:   version,
		Data:      data,
		Timestamp: time.Now(),
		Ctx:       context.Background(),
	}
}

// NewEvent creates a new acknowledged event as a response to this command.
func (command *Command) NewEvent(action string, version int8, data []byte) Event {
	event := NewEvent(action, version, command.ID, command.Key, data)
	event.CommandTimestamp = command.Timestamp
	event.Ctx = command.Ctx

	return event
}

// NewError creates a error event as a response to this command.
func (command *Command) NewError(action string, status StatusCode, err error) Event {
	event := NewEvent(action, 0, command.ID, command.Key, nil)
	event.Status = status
	event.CommandTimestamp = command.Timestamp
	event.Ctx = command.Ctx

	if err != nil {
		event.Meta = err.Error()
	}

	return event
}

// Populate populates the command struct with the given message.
// If a error occures during the parsing of the message is the error silently thrown and returned.
// When a error is thrown is will also a log be logged if an output writer is given to the commander logger.
func (command *Command) Populate(message *Message) error {
	Logger.Println("Populating a command from a message")

	command.Headers = make(map[string]string)
	var throw error

headers:
	for key, value := range message.Headers {
		str := string(value)

		switch key {
		case ActionHeader:
			command.Action = str
			continue headers
		case IDHeader:
			id, err := uuid.FromString(str)

			if err != nil {
				throw = err
				continue
			}

			command.ID = id
			continue headers
		case VersionHeader:
			version, err := strconv.ParseInt(str, 10, 8)
			if err != nil {
				throw = err
				continue
			}

			command.Version = int8(version)
			continue headers
		case EOSHeader:
			if value == "1" {
				command.EOS = true
			}
			continue headers
		}

		command.Headers[key] = str
	}

	if len(command.Action) == 0 {
		return errors.New("No command action is set")
	}

	command.Key = message.Key
	command.Data = message.Value
	command.Origin = message.Topic
	command.Offset = message.Offset
	command.Partition = message.Partition
	command.Timestamp = message.Timestamp

	if throw != nil {
		Logger.Println("A error was thrown when populating the command message:", throw)
	}

	return throw
}

// Message constructs a new commander message for the given command
func (command *Command) Message(topic Topic) *Message {
	headers := make(map[string]string)
	for key, value := range command.Headers {
		headers[key] = value
	}

	eos := "0"
	if command.EOS {
		eos = "1"
	}

	headers[ActionHeader] = command.Action
	headers[IDHeader] = command.ID.String()
	headers[CommandTimestampHeader] = strconv.Itoa(int(command.Timestamp.UnixNano()))
	headers[EOSHeader] = eos

	message := &Message{
		Headers:   headers,
		Key:       command.Key,
		Value:     command.Data,
		Offset:    command.Offset,
		Partition: command.Partition,
		Topic:     topic,
		Ctx:       command.Ctx,
	}

	return message
}
