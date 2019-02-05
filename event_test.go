package commander

import (
	"testing"

	"github.com/gofrs/uuid"
)

// NewMockEvent produces a new mock command with the given action
func NewMockEvent(action string) *Event {
	headers := make(map[string]string)

	parent, _ := uuid.NewV4()
	key, _ := uuid.NewV4()
	id, _ := uuid.NewV4()

	event := &Event{
		Headers: headers,
		Parent:  parent,
		Key:     key,
		ID:      id,
		Status:  StatusOK,
		Origin:  Topic{Name: "topic"},
		Action:  action,
		Data:    []byte("{}"),
	}

	return event
}

// TestEventPopulation tests if able to populate a event from a kafka message
func TestEventPopulation(t *testing.T) {
	action := "action"
	version := 1
	parent, _ := uuid.NewV4()
	key, _ := uuid.NewV4()
	id, _ := uuid.NewV4()

	message := NewMockEventMessage(action, version, parent.String(), key.String(), id.String(), "{}", Topic{})

	event := &Event{}
	event.Populate(&message)

	if event.Action != action {
		t.Error("The populated event action is not set correctly")
	}

	if event.ID.String() != id.String() {
		t.Error("The populated event id is not set correctly")
	}

	if event.Key.String() != key.String() {
		t.Error("The populated event key is not set correctly")
	}

	if event.Parent.String() != parent.String() {
		t.Error("The populated event parent is not set correctly")
	}

	if event.Version != version {
		t.Error("The populated event version is not set correctly")
	}
}

// TestErrorHandlingEventPopulation tests if errors are thrown when populating a event
func TestErrorHandlingEventPopulation(t *testing.T) {
	var err error
	var corrupted Message
	event := &Event{}

	action := "action"
	version := 1
	parent, _ := uuid.NewV4()
	key, _ := uuid.NewV4()
	id, _ := uuid.NewV4()
	value := "{}"

	corrupted = NewMockEventMessage(action, version, parent.String(), key.String(), id.String(), value, Topic{Name: "testing"})
	corrupted.Key = []byte("")

	err = event.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted key population")
	}

	corrupted = NewMockEventMessage(action, version, parent.String(), key.String(), id.String(), value, Topic{Name: "testing"})
	for index, header := range corrupted.Headers {
		if header.Key == IDHeader {
			corrupted.Headers[index].Value = []byte("")
		}
	}

	err = event.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted id population")
	}

	corrupted = NewMockEventMessage(action, version, parent.String(), key.String(), id.String(), value, Topic{Name: "testing"})
	for index, header := range corrupted.Headers {
		if header.Key == ActionHeader {
			corrupted.Headers[index].Value = []byte("")
		}
	}

	err = event.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted action population")
	}

	corrupted = NewMockEventMessage(action, version, parent.String(), key.String(), id.String(), value, Topic{Name: "testing"})
	for index, header := range corrupted.Headers {
		if header.Key == ParentHeader {
			corrupted.Headers[index].Value = []byte("")
		}
	}

	err = event.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted parent population")
	}

	corrupted = NewMockEventMessage(action, version, parent.String(), key.String(), id.String(), value, Topic{Name: "testing"})
	for index, header := range corrupted.Headers {
		if header.Key == VersionHeader {
			corrupted.Headers[index].Value = []byte("")
		}
	}

	err = event.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted version population")
	}
}
