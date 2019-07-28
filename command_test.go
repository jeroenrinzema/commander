package commander

import (
	"errors"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander/types"
)

// NewMockCommand produces a new mock command with the given action
func NewMockCommand(action string) Command {
	id := uuid.Must(uuid.NewV4()).String()
	key := types.Key([]byte(id))

	command := Command{
		ID:     id,
		Origin: Topic{Name: "topic"},
		Key:    key,
		Action: action,
	}

	return command
}

// TestCommandEventConstruction tests if able to construct a event of a command
func TestCommandEventConstruction(t *testing.T) {
	command := NewMockCommand("action")

	action := "event"
	version := types.Version(1)

	event := command.NewEvent(action, version, nil)

	if event.Action != action {
		t.Error("Event action does not match")
	}

	if event.Version != version {
		t.Error("Event version does not match given version")
	}

	if event.Parent != types.ParentID(command.ID) {
		t.Error("Event does not have id of command")
	}

	if event.Status != StatusOK {
		t.Error("Event is not acknowledged")
	}
}

// TestCommandErrorEventConstruction tests if able to construct a error event
func TestCommandErrorEventConstruction(t *testing.T) {
	command := NewMockCommand("action")
	event := command.NewError("event", StatusImATeapot, errors.New("test error"))

	if event.Parent != types.ParentID(command.ID) {
		t.Error("Event does not have id of command")
	}

	if event.Status == StatusOK {
		t.Error("Error event is acknowledged")
	}
}

// TestCommandPopulation tests if able to populate a command from a kafka message
func TestCommandPopulation(t *testing.T) {
	action := "action"

	id := uuid.Must(uuid.NewV4()).String()
	key := types.Key([]byte(id))

	message := NewMockCommandMessage("action", key, id, nil, Topic{Name: "testing"})
	command := NewCommandFromMessage(message)

	if command.Action != action {
		t.Error("The populated command action is not set correctly")
	}

	if command.ID != id {
		t.Error("The populated command id is not set correctly")
	}

	if string(command.Key) != string(key) {
		t.Error("The populated command key is not set correctly")
	}
}
