package commander

import (
	"testing"

	"github.com/gofrs/uuid"
)

// NewMockCommand produces a new mock command with the given action
func NewMockCommand(action string) *Command {
	headers := make(map[string]string)

	key, _ := uuid.NewV4()
	id, _ := uuid.NewV4()

	command := &Command{
		Origin:  Topic{Name: "topic"},
		Key:     key,
		Headers: headers,
		ID:      id,
		Action:  action,
		Data:    []byte("{}"),
	}

	return command
}

// TestCommandEventConstruction tests if able to construct a event of a command
func TestCommandEventConstruction(t *testing.T) {
	command := NewMockCommand("action")

	action := "event"
	version := int8(1)

	event := command.NewEvent(action, version, []byte("{}"))

	if event.Action != action {
		t.Error("Ecent action does not match")
	}

	if event.Version != version {
		t.Error("Event version does not match given version")
	}

	if event.Parent != command.ID {
		t.Error("Event does not have id of command")
	}

	if event.Status != StatusOK {
		t.Error("Event is not acknowledged")
	}
}

// TestCommandErrorEventConstruction tests if able to construct a error event
func TestCommandErrorEventConstruction(t *testing.T) {
	command := NewMockCommand("action")
	event := command.NewError("event", []byte("{}"))

	if event.Parent != command.ID {
		t.Error("Event does not have id of command")
	}

	if event.Status == StatusOK {
		t.Error("Error event is acknowledged")
	}
}

// TestCommandPopulation tests if able to populate a command from a kafka message
func TestCommandPopulation(t *testing.T) {
	action := "action"
	key, _ := uuid.NewV4()
	id, _ := uuid.NewV4()

	message := NewMockCommandMessage("action", key.String(), id.String(), "{}", Topic{Name: "testing"})

	command := &Command{}
	command.Populate(&message)

	if command.Action != action {
		t.Error("The populated command action is not set correctly")
	}

	if command.ID.String() != id.String() {
		t.Error("The populated command id is not set correctly")
	}

	if command.Key.String() != key.String() {
		t.Error("The populated command key is not set correctly")
	}
}

// TestErrorHandlingCommandPopulation tests if errors are thrown when populating a command
func TestErrorHandlingCommandPopulation(t *testing.T) {
	var err error
	var corrupted Message
	command := &Command{}

	action := "action"
	key, _ := uuid.NewV4()
	id, _ := uuid.NewV4()
	value := "{}"

	corrupted = NewMockCommandMessage(action, key.String(), id.String(), value, Topic{Name: "testing"})
	corrupted.Key = []byte("")

	err = command.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted key population")
	}

	corrupted = NewMockCommandMessage(action, key.String(), id.String(), value, Topic{Name: "testing"})
	for index, header := range corrupted.Headers {
		if header.Key == IDHeader {
			corrupted.Headers[index].Value = []byte("")
		}
	}

	err = command.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted id population")
	}

	corrupted = NewMockCommandMessage(action, key.String(), id.String(), value, Topic{Name: "testing"})
	for index, header := range corrupted.Headers {
		if header.Key == ActionHeader {
			corrupted.Headers[index].Value = []byte("")
		}
	}

	err = command.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted action population")
	}
}
