package commander

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

// NewMockCommandKafkaMessage produces a mock command message
func NewMockCommandKafkaMessage(action string, key string, id string, value string) kafka.Message {
	topic := "topic"

	message := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic,
		},
		Key:       []byte(key),
		Value:     []byte(value),
		Timestamp: time.Now(),
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(action),
			},
			kafka.Header{
				Key:   IDHeader,
				Value: []byte(id),
			},
		},
	}

	return message
}

// NewMockCommand produces a new mock command with the given action
func NewMockCommand(action string) *Command {
	headers := make(map[string]string)

	command := &Command{
		Origin:  "topic",
		Key:     uuid.NewV4(),
		Headers: headers,
		ID:      uuid.NewV4(),
		Action:  action,
		Data:    []byte("{}"),
	}

	return command
}

// TestCommandEventConstruction tests if able to construct a event of a command
func TestCommandEventConstruction(t *testing.T) {
	command := NewMockCommand("action")

	action := "event"
	version := 1
	key := uuid.NewV4()

	event := command.NewEvent(action, version, key, []byte("{}"))

	if event.Action != action {
		t.Error("Ecent action does not match")
	}

	if event.Version != version {
		t.Error("Event version does not match given version")
	}

	if event.Parent != command.ID {
		t.Error("Event does not have id of command")
	}

	if !event.Acknowledged {
		t.Error("Event is not acknowledged")
	}
}

// TestCommandErrorEventConstruction tests if able to construct a error event
func TestCommandErrorEventConstruction(t *testing.T) {
	command := NewMockCommand("action")
	event := command.NewErrorEvent("event", []byte("{}"))

	if event.Parent != command.ID {
		t.Error("Event does not have id of command")
	}

	if event.Acknowledged {
		t.Error("Error event is acknowledged")
	}
}

// TestCommandPopulation tests if able to populate a command from a kafka message
func TestCommandPopulation(t *testing.T) {
	action := "action"
	key := uuid.NewV4().String()
	id := uuid.NewV4().String()

	message := NewMockCommandKafkaMessage("action", key, id, "{}")

	command := &Command{}
	command.Populate(&message)

	if command.Action != action {
		t.Error("The populated command action is not set correctly")
	}

	if command.ID.String() != id {
		t.Error("The populated command id is not set correctly")
	}

	if command.Key.String() != key {
		t.Error("The populated command key is not set correctly")
	}
}

// TestErrorHandlingCommandPopulation tests if errors are thrown when populating a command
func TestErrorHandlingCommandPopulation(t *testing.T) {
	var err error
	var corrupted kafka.Message
	command := &Command{}

	action := "action"
	key := uuid.NewV4().String()
	id := uuid.NewV4().String()
	value := "{}"

	corrupted = NewMockCommandKafkaMessage(action, key, id, value)
	corrupted.Key = []byte("")

	err = command.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted key population")
	}

	corrupted = NewMockCommandKafkaMessage(action, key, id, value)
	for index, header := range corrupted.Headers {
		if header.Key == IDHeader {
			corrupted.Headers[index].Value = []byte("")
		}
	}

	err = command.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted id population")
	}

	corrupted = NewMockCommandKafkaMessage(action, key, id, value)
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
