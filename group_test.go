package commander

import (
	"context"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
)

type actionHandle struct {
	messages chan interface{}
}

func (handle *actionHandle) Process(writer ResponseWriter, message interface{}) {
	handle.messages <- message
}

// NewTestGroup initializes a new group used for testing
func NewTestGroup() *Group {
	client, _ := NewTestClient()
	group := &Group{
		Client:  client,
		Timeout: 5 * time.Second,
		Topics: []Topic{
			Topic{
				Name:    "",
				Type:    EventTopic,
				Consume: true,
				Produce: true,
			},
			Topic{
				Name:    "",
				Type:    CommandTopic,
				Consume: true,
				Produce: true,
			},
		},
	}

	return group
}

// TestProduceCommand tests if able to produce a command
func TestProduceCommand(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	key := uuid.NewV4()
	command := NewCommand("testing", key, []byte("{}"))

	group.ProduceCommand(command)
}

// TestProduceEvent tests if able to produce a event
func TestProduceEvent(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	key := uuid.NewV4()
	parent := uuid.NewV4()
	event := NewEvent("tested", 1, parent, key, []byte("{}"))

	group.ProduceEvent(event)
}

// TestAsyncCommand tests if plausible to create a async command
func TestAsyncCommand(t *testing.T) {
	group := NewTestGroup()
	command := NewMockCommand("action")
	NewTestClient(group)

	err := group.AsyncCommand(command)

	if err != nil {
		t.Error(err)
	}
}

// TestSyncCommand tests if able to send a sync command
func TestSyncCommand(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	command := NewCommand("testing", uuid.NewV4(), []byte("{}"))

	go func() {
		_, err := group.SyncCommand(command)
		if err != nil {
			t.Error(err)
		}
	}()

	event := NewEvent("tested", 1, command.ID, command.Key, []byte("{}"))
	group.ProduceEvent(event)
}

// TestAwaitEvent tests if plausible to await a event
func TestAwaitEvent(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	timeout := 2 * time.Second
	parent := uuid.NewV4()

	go func() {
		_, err := group.AwaitEvent(timeout, parent)
		if err != nil {
			t.Error(err)
		}
	}()

	event := NewEvent("tested", 1, parent, uuid.Nil, []byte("{}"))
	group.ProduceEvent(event)
}

// TestEventConsumer tests if events get consumed
func TestEventConsumer(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	go func() {
		deadline := time.Now().Add(500 * time.Millisecond)

		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		events, close, err := group.NewConsumer(EventTopic)
		if err != nil {
			panic(err)
		}

		defer cancel()
		defer close()

		select {
		case <-events:
		case <-ctx.Done():
			t.Error("no message was consumed within the deadline")
		}
	}()

	parent := uuid.NewV4()
	key := uuid.NewV4()

	event := NewEvent("tested", 1, parent, key, []byte("{}"))
	group.ProduceEvent(event)
}

// TestCommandConsumer tests if commands get consumed
func TestCommandConsumer(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	go func() {
		deadline := time.Now().Add(500 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)

		commands, close, err := group.NewConsumer(CommandTopic)
		if err != nil {
			panic(err)
		}

		defer cancel()
		defer close()

		select {
		case <-commands:
		case <-ctx.Done():
			t.Error("no message was consumed within the deadline")
		}
	}()

	command := NewCommand("testing", uuid.NewV4(), []byte("{}"))
	group.ProduceCommand(command)
}

// TestEventHandleFunc tests if a event handle func get's called
func TestEventHandleFunc(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	action := "testing"
	delivered := make(chan *Event, 1)

	group.HandleFunc(action, EventTopic, func(writer ResponseWriter, message interface{}) {
		event, ok := message.(*Event)
		if !ok {
			t.Error("the received message is not a event")
		}

		delivered <- event
	})

	event := NewEvent("tested", 1, uuid.NewV4(), uuid.NewV4(), []byte("{}"))
	group.ProduceEvent(event)

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-delivered:
	case <-ctx.Done():
		t.Error("the events handle was not called within the deadline")
	}
}

// TestEventHandle testis if a event handle get's called
func TestEventHandle(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	action := "testing"
	delivered := make(chan interface{}, 1)

	handle := &actionHandle{delivered}
	group.Handle(action, EventTopic, handle)

	event := NewEvent(action, 1, uuid.NewV4(), uuid.NewV4(), []byte("{}"))
	group.ProduceEvent(event)

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-delivered:
	case <-ctx.Done():
		t.Error("the events handle was not called within the deadline")
	}
}
