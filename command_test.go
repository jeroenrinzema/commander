package commander

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

const (
	TestAction  = "create"
	TestVersion = 1
)

var (
	TestKey            = uuid.NewV4()
	TestID             = uuid.NewV4()
	TestCommandMessage = kafka.Message{
		Key:       []byte(TestKey.String()),
		Value:     []byte("{}"),
		Timestamp: time.Now(),
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(TestAction),
			},
			kafka.Header{
				Key:   IDHeader,
				Value: []byte(TestID.String()),
			},
		},
	}
)

func NewCommand() *Command {
	headers := make(map[string]string)
	headers["acknowledged"] = "true"

	command := &Command{
		Key:     uuid.NewV4(),
		Headers: headers,
		ID:      uuid.NewV4(),
		Action:  TestAction,
		Data:    []byte("{}"),
	}

	return command
}

func TestCommandEventConstruction(t *testing.T) {
	command := NewCommand()
	key := uuid.NewV4()

	event := command.NewEvent(TestAction, TestVersion, key, []byte("{}"))

	if event.Version != TestVersion {
		t.Error("Event version does not match given version")
	}

	if event.Parent != command.ID {
		t.Error("Event does not have id of command")
	}

	if !event.Acknowledged {
		t.Error("Event is not acknowledged")
	}
}

func TestCommandErrorEventConstruction(t *testing.T) {
	command := NewCommand()
	event := command.NewErrorEvent(TestAction, []byte("{}"))

	if event.Parent != command.ID {
		t.Error("Event does not have id of command")
	}

	if event.Acknowledged {
		t.Error("Error event is acknowledged")
	}
}

func TestCommandPopulation(t *testing.T) {
	command := &Command{}
	command.Populate(&TestCommandMessage)

	if command.Action != TestAction {
		t.Error("The populated command action is not set correctly")
	}

	if command.ID.String() != TestID.String() {
		t.Error("The populated command id is not set correctly")
	}

	if command.Key.String() != TestKey.String() {
		t.Error("The populated command key is not set correctly")
	}
}

func TestErrorHandlingCommandPopulation(t *testing.T) {
	var err error
	var corrupted kafka.Message
	command := &Command{}

	corrupted = TestCommandMessage
	corrupted.Key = []byte("")

	err = command.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted key population")
	}

	corrupted = TestCommandMessage
	for index, header := range corrupted.Headers {
		if header.Key == IDHeader {
			corrupted.Headers[index].Value = []byte("")
		}
	}

	err = command.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted id population")
	}
}
