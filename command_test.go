package commander

import (
	"encoding/json"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

func TestAcknowledgedEventCreation(t *testing.T) {
	type data struct {
		Name string `json:"name"`
	}

	user := data{"jeroen"}
	key := uuid.NewV4()
	payload, MarshalErr := json.Marshal(user)

	if MarshalErr != nil {
		t.Error(MarshalErr)
	}

	command := NewCommand("test_action", payload)
	event := command.NewEvent("test_event", 1, key, payload)

	if !event.Acknowledged {
		t.Error("event should be acknowledged")
	}

	if event.Key == uuid.Nil {
		t.Error("event should have a key")
	}
}

func TestErrorEventCreation(t *testing.T) {
	type data struct {
		Name string `json:"name"`
	}

	user := data{"jeroen"}
	payload, MarshalErr := json.Marshal(user)

	if MarshalErr != nil {
		t.Error(MarshalErr)
	}

	command := NewCommand("test_action", payload)
	event := command.NewError("test_error_event", payload)

	if event.Acknowledged {
		t.Error("event should not be acknowledged")
	}

	if event.Key != uuid.Nil {
		t.Error("event key is not nil")
	}
}

func TestCommandPopulate(t *testing.T) {
	type data struct {
		Name string `json:"name"`
	}

	id := uuid.NewV4()
	action := "test_action"

	user := data{"jeroen"}
	payload, MarshalErr := json.Marshal(user)

	if MarshalErr != nil {
		t.Error(MarshalErr)
	}

	command := Command{}
	CompleteMessage := kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(action),
			},
		},
		Key:   id.Bytes(),
		Value: payload,
	}

	var PopulateErr error

	PopulateErr = command.Populate(&CompleteMessage)
	if PopulateErr != nil {
		t.Error(PopulateErr)
	}

	CorruptedKeyMessage := kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(action),
			},
		},
		Key:   []byte("faulty"),
		Value: payload,
	}

	PopulateErr = command.Populate(&CorruptedKeyMessage)
	if PopulateErr == nil {
		t.Error("no error is thrown when the message key is corrupted")
	}
}
