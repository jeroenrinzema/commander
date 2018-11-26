package commander

import (
	"testing"

	uuid "github.com/satori/go.uuid"
)

// NewMockEvent produces a new mock command with the given action
func NewMockEvent(action string) *Event {
	headers := make(map[string]string)

	event := &Event{
		Headers:      headers,
		Parent:       uuid.NewV4(),
		Key:          uuid.NewV4(),
		ID:           uuid.NewV4(),
		Acknowledged: true,
		Origin:       Topic{Name: "topic"},
		Action:       action,
		Data:         []byte("{}"),
	}

	return event
}

// TestEventPopulation tests if able to populate a event from a kafka message
func TestEventPopulation(t *testing.T) {
	action := "action"
	version := 1
	parent := uuid.NewV4().String()
	key := uuid.NewV4().String()
	id := uuid.NewV4().String()

	message := NewMockEventMessage(action, version, parent, key, id, "{}", Topic{})

	event := &Event{}
	event.Populate(&message)

	if event.Action != action {
		t.Error("The populated event action is not set correctly")
	}

	if event.ID.String() != id {
		t.Error("The populated event id is not set correctly")
	}

	if event.Key.String() != key {
		t.Error("The populated event key is not set correctly")
	}

	if event.Parent.String() != parent {
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
	parent := uuid.NewV4().String()
	key := uuid.NewV4().String()
	id := uuid.NewV4().String()
	value := "{}"

	corrupted = NewMockEventMessage(action, version, parent, key, id, value, Topic{Name: "testing"})
	corrupted.Key = []byte("")

	err = event.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted key population")
	}

	corrupted = NewMockEventMessage(action, version, parent, key, id, value, Topic{Name: "testing"})
	for index, header := range corrupted.Headers {
		if header.Key == IDHeader {
			corrupted.Headers[index].Value = []byte("")
		}
	}

	err = event.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted id population")
	}

	corrupted = NewMockEventMessage(action, version, parent, key, id, value, Topic{Name: "testing"})
	for index, header := range corrupted.Headers {
		if header.Key == ActionHeader {
			corrupted.Headers[index].Value = []byte("")
		}
	}

	err = event.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted action population")
	}

	corrupted = NewMockEventMessage(action, version, parent, key, id, value, Topic{Name: "testing"})
	for index, header := range corrupted.Headers {
		if header.Key == ParentHeader {
			corrupted.Headers[index].Value = []byte("")
		}
	}

	err = event.Populate(&corrupted)
	if err == nil {
		t.Error("no error is thrown during corrupted parent population")
	}

	corrupted = NewMockEventMessage(action, version, parent, key, id, value, Topic{Name: "testing"})
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
