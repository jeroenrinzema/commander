package commander

import (
	"testing"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander/types"
)

// NewMockEvent produces a new mock command with the given action
func NewMockEvent(action string) *Event {
	id := uuid.Must(uuid.NewV4()).String()
	key := types.Key([]byte(id))
	parent := types.ParentID(uuid.Must(uuid.NewV4()).String())

	event := &Event{
		Parent: parent,
		Key:    key,
		ID:     id,
		Status: StatusOK,
		Origin: Topic{Name: "topic"},
		Action: action,
		Data:   nil,
	}

	return event
}

// TestEventPopulation tests if able to populate a event from a kafka message
func TestEventPopulation(t *testing.T) {
	action := "action"
	version := types.Version(1)
	parent := types.ParentID(uuid.Must(uuid.NewV4()).String())
	id := uuid.Must(uuid.NewV4()).String()
	key := types.Key([]byte(id))

	message := NewMockEventMessage(action, version, parent, key, id, nil, Topic{})
	event := NewEventFromMessage(message)

	if event.Action != action {
		t.Error("The populated event action is not set correctly")
	}

	if event.ID != id {
		t.Error("The populated event id is not set correctly")
	}

	if string(event.Key) != string(key) {
		t.Error("The populated event key is not set correctly")
	}

	if event.Parent != parent {
		t.Error("The populated event parent is not set correctly")
	}

	if event.Version != version {
		t.Error("The populated event version is not set correctly")
	}
}
