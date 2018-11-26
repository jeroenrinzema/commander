package commander

import (
	"context"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
)

// TestNewResponseWriter tests if able to construct a new resoponse writer
func TestNewResponseWriter(t *testing.T) {
	group := NewTestGroup()
	command := NewMockCommand("testing")

	NewTestClient(group)

	NewResponseWriter(group, command)
	NewResponseWriter(group, nil)
}

// TestWriterProduceCommand tests if able to write a command to the mock group
func TestWriterProduceCommand(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	action := "testing"
	key := uuid.NewV4()
	data := []byte("{}")

	command := NewMockCommand(action)
	writer := NewResponseWriter(group, command)

	messages, closing, err := group.NewConsumer(CommandTopic)
	if err != nil {
		t.Error(err)
		return
	}

	defer closing()

	if _, err := writer.ProduceCommand(action, key, data); err != nil {
		t.Error(err)
		return
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-messages:
	case <-ctx.Done():
		t.Error("the events handle was not called within the deadline")
	}
}

// TestWriterProduceEvent tests if able to write a event to the mock group
func TestWriterProduceEvent(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	action := "testing"
	key := uuid.NewV4()
	version := 1
	data := []byte("{}")

	command := NewMockCommand(action)
	writer := NewResponseWriter(group, command)

	messages, closing, err := group.NewConsumer(EventTopic)
	if err != nil {
		t.Error(err)
		return
	}

	defer closing()

	if _, err := writer.ProduceEvent(action, version, key, data); err != nil {
		t.Error(err)
		return
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case message := <-messages:
		event := &Event{}
		event.Populate(message)

		if event.Parent != command.ID {
			t.Error("The event parent does not match the command id")
		}
	case <-ctx.Done():
		t.Error("the events handle was not called within the deadline")
	}

}

// TestWriterProduceErrorEvent tests if able to write a error event to the mock group
func TestWriterProduceErrorEvent(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	action := "testing"
	data := []byte("{}")

	command := NewMockCommand(action)
	writer := NewResponseWriter(group, command)

	messages, closing, err := group.NewConsumer(EventTopic)
	if err != nil {
		t.Error(err)
		return
	}

	defer closing()

	if _, err := writer.ProduceError(action, data); err != nil {
		t.Error(err)
		return
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-messages:
	case <-ctx.Done():
		t.Error("the events handle was not called within the deadline")
	}
}
