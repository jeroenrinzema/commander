package commander

import (
	"context"
	"testing"
	"time"

	"github.com/gofrs/uuid"
)

type actionHandle struct {
	messages chan interface{}
}

func (handle *actionHandle) Process(writer ResponseWriter, message interface{}) {
	handle.messages <- message
}

// NewTestGroup initializes a new group used for testing
func NewTestGroup() *Group {
	group := &Group{
		Timeout: 5 * time.Second,
		Topics: []Topic{
			Topic{
				Name:    "events",
				Type:    EventTopic,
				Consume: true,
				Produce: true,
			},
			Topic{
				Name:    "commands",
				Type:    CommandTopic,
				Consume: true,
				Produce: true,
			},
		},
	}

	NewTestClient(group)
	return group
}

// TestProduceCommand tests if able to produce a command
func TestProduceCommand(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	key, _ := uuid.NewV4()
	command := NewCommand("testing", 1, key, []byte("{}"))

	group.ProduceCommand(command)
}

// TestProduceEvent tests if able to produce a event
func TestProduceEvent(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	key, _ := uuid.NewV4()
	parent, _ := uuid.NewV4()
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

	key, _ := uuid.NewV4()
	command := NewCommand("testing", 1, key, []byte("{}"))

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
	parent, _ := uuid.NewV4()

	go func() {
		sink, err := group.AwaitEvent(timeout, parent)
		select {
		case e := <-err:
			t.Error(e)
		case <-sink:
		}
	}()

	event := NewEvent("tested", 1, parent, uuid.Nil, []byte("{}"))
	group.ProduceEvent(event)
}

// TestEventConsumer tests if events get consumed
func TestEventConsumer(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	events, close, err := group.NewConsumer(EventTopic)
	if err != nil {
		panic(err)
	}

	defer close()

	parent, _ := uuid.NewV4()
	key, _ := uuid.NewV4()

	event := NewEvent("tested", 1, parent, key, []byte("{}"))
	group.ProduceEvent(event)

	deadline := time.Now().Add(500 * time.Millisecond)

	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-events:
	case <-ctx.Done():
		t.Error("no message was consumed within the deadline")
	}
}

// TestCommandConsumer tests if commands get consumed
func TestCommandConsumer(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	commands, close, err := group.NewConsumer(CommandTopic)
	if err != nil {
		panic(err)
	}

	defer close()

	key, _ := uuid.NewV4()
	command := NewCommand("testing", 1, key, []byte("{}"))
	group.ProduceCommand(command)

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-commands:
	case <-ctx.Done():
		t.Error("no message was consumed within the deadline")
	}
}

// TestEventHandleFunc tests if a event handle func get's called
func TestEventHandleFunc(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	action := "testing"
	delivered := make(chan *Event, 1)

	group.HandleFunc(EventTopic, action, func(writer ResponseWriter, message interface{}) {
		event, ok := message.(*Event)
		if !ok {
			t.Error("the received message is not a event")
		}

		delivered <- event
	})

	parent, _ := uuid.NewV4()
	key, _ := uuid.NewV4()

	event := NewEvent(action, 1, parent, key, []byte("{}"))
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
	group.Handle(EventTopic, action, handle)

	parent, _ := uuid.NewV4()
	key, _ := uuid.NewV4()

	event := NewEvent(action, 1, parent, key, []byte("{}"))
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

// TestActionEventHandle tests if event actions are isolated
func TestActionEventHandle(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	delivered := make(chan interface{}, 1)
	failure := make(chan interface{}, 1)

	group.Handle(EventTopic, "create", &actionHandle{delivered})
	group.Handle(EventTopic, "delete", &actionHandle{failure})

	parent, _ := uuid.NewV4()
	key, _ := uuid.NewV4()

	event := NewEvent("create", 1, parent, key, []byte("{}"))
	group.ProduceEvent(event)

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-delivered:
	case <-failure:
		t.Error("the event action was not isolated")
	case <-ctx.Done():
		t.Error("the events handle was not called within the deadline")
	}
}

// TestActionCommandHandle tests if command actions are isolated
func TestActionCommandHandle(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	delivered := make(chan interface{}, 1)
	failure := make(chan interface{}, 1)

	group.Handle(CommandTopic, "delete", &actionHandle{failure})
	group.Handle(CommandTopic, "create", &actionHandle{delivered})

	key, _ := uuid.NewV4()

	command := NewCommand("create", 1, key, []byte("{}"))
	group.ProduceCommand(command)

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-delivered:
	case <-failure:
		t.Error("the event action was not isolated")
	case <-ctx.Done():
		t.Error("the events handle was not called within the deadline")
	}
}
