package commander

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gofrs/uuid"
)

// TestNewResponseWriter tests if able to construct a new resoponse writer
func TestNewResponseWriter(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	command := NewMockCommand("testing")

	NewResponseWriter(group, command)
	NewResponseWriter(group, nil)
}

// TestWriterProduceCommand tests if able to write a command to the mock group
func TestWriterProduceCommand(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	action := "testing"
	key, _ := uuid.NewV4()
	data := []byte("{}")

	command := NewMockCommand(action)
	writer := NewResponseWriter(group, command)

	messages, marked, closing, err := group.NewConsumer(CommandMessage)
	if err != nil {
		t.Error(err)
		return
	}

	defer closing()

	if _, err := writer.ProduceCommand(action, 1, key, data); err != nil {
		t.Error(err)
		return
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-messages:
		marked <- nil
	case <-ctx.Done():
		t.Error("the events handle was not called within the deadline")
	}
}

// TestWriterProduceEvent tests if able to write a event to the mock group
func TestWriterProduceEvent(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	action := "testing"
	key, _ := uuid.NewV4()
	version := int8(1)
	data := []byte("{}")

	command := NewMockCommand(action)
	writer := NewResponseWriter(group, command)

	messages, marked, closing, err := group.NewConsumer(EventMessage)
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
		event := Event{}
		event.Populate(message)

		if event.Parent != command.ID {
			t.Error("The event parent does not match the command id")
		}

		marked <- nil
	case <-ctx.Done():
		t.Error("the events handle was not called within the deadline")
	}

}

// TestWriterProduceErrorEvent tests if able to write a error event to the mock group
func TestWriterProduceErrorEvent(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	action := "testing"
	data := errors.New("test error")

	command := NewMockCommand(action)
	writer := NewResponseWriter(group, command)

	messages, marked, closing, err := group.NewConsumer(EventMessage)
	if err != nil {
		t.Error(err)
		return
	}

	defer closing()

	if _, err := writer.ProduceError(action, StatusImATeapot, data); err != nil {
		t.Error(err)
		return
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-messages:
		marked <- nil
	case <-ctx.Done():
		t.Error("the events handle was not called within the deadline")
	}
}
