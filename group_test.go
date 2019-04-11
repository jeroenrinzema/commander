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

// NewMockClient initializes a new in-memory mocking dialect, group and commander client used for testing
func NewMockClient() (*Group, *Client) {
	dialect := NewMockDialect()
	group := NewGroup(
		NewTopic("events", dialect, EventMessage, ConsumeMode|ProduceMode),
		NewTopic("commands", dialect, CommandMessage, ConsumeMode|ProduceMode),
	)

	client := NewClient(group)
	return group, client
}

// TestProduceCommand tests if able to produce a command
func TestProduceCommand(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	key, _ := uuid.NewV4()
	command := NewCommand("testing", 1, key, []byte("{}"))

	group.ProduceCommand(command)
}

// TestProduceEvent tests if able to produce a event
func TestProduceEvent(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	key, _ := uuid.NewV4()
	parent, _ := uuid.NewV4()
	event := NewEvent("tested", 1, parent, key, []byte("{}"))

	group.ProduceEvent(event)
}

// TestProduceEventNoTopics tests if a error is returned when no topics for production are found
func TestProduceEventNoTopics(t *testing.T) {
	group := NewGroup()

	client := NewClient(group)
	defer client.Close()

	key, _ := uuid.NewV4()
	parent, _ := uuid.NewV4()
	event := NewEvent("tested", 1, parent, key, []byte("{}"))

	err := group.ProduceEvent(event)
	if err != ErrNoTopic {
		t.Fatal("no topic error is not thrown when expected")
	}
}

// TestAsyncCommand tests if plausible to create a async command
func TestAsyncCommand(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	command := NewMockCommand("action")
	err := group.AsyncCommand(command)

	if err != nil {
		t.Error(err)
	}
}

// TestSyncCommand tests if able to send a sync command
func TestSyncCommand(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	action := "testing"
	key, _ := uuid.NewV4()
	command := NewCommand(action, 1, key, []byte("{}"))

	messages, marked, closing, err := group.NewConsumer(CommandMessage)
	if err != nil {
		t.Fatal(err)
	}

	defer closing()
	delivered := make(chan Event, 1)

	go func() {
		event, err := group.SyncCommand(command)
		if err != nil {
			t.Fatal(err)
		}

		delivered <- event
	}()

	<-messages

	event := NewEvent(action, 1, command.ID, uuid.Nil, nil)
	err = group.ProduceEvent(event)
	if err != nil {
		marked <- err
		t.Fatal(err)
	}

	marked <- nil
	event = <-delivered

	if event.Parent != command.ID {
		t.Fatal("command id and parent do not match")
	}
}

// TestAwaitEvent tests if plausible to await a event
func TestAwaitEvent(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	timeout := 1 * time.Second
	parent, _ := uuid.NewV4()

	sink, marked, errs := group.AwaitEvent(timeout, parent)

	event := NewEvent("tested", 1, parent, uuid.Nil, []byte("{}"))
	err := group.ProduceEvent(event)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case e := <-errs:
		t.Fatal(e)
	case <-sink:
		marked <- nil
	}
}

// TestEventConsumer tests if events get consumed
func TestEventConsumer(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	events, marked, close, err := group.NewConsumer(EventMessage)
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

	marked <- nil
}

// TestCommandConsumer tests if commands get consumed
func TestCommandConsumer(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	commands, marked, close, err := group.NewConsumer(CommandMessage)
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

	marked <- nil
}

// TestEventHandleFunc tests if a event handle func get's called
func TestEventHandleFunc(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	action := "testing"
	delivered := make(chan Event, 1)

	group.HandleFunc(EventMessage, action, func(writer ResponseWriter, message interface{}) {
		event, ok := message.(Event)
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
	group, client := NewMockClient()
	defer client.Close()

	action := "testing"
	delivered := make(chan interface{}, 1)

	handle := &actionHandle{delivered}
	group.Handle(EventMessage, action, handle)

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
	group, client := NewMockClient()
	defer client.Close()

	delivered := make(chan interface{}, 2)
	failure := make(chan interface{}, 2)

	group.Handle(EventMessage, "create", &actionHandle{delivered})
	group.Handle(EventMessage, "delete", &actionHandle{failure})

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
	group, client := NewMockClient()
	defer client.Close()

	delivered := make(chan interface{}, 1)
	failure := make(chan interface{}, 1)

	group.Handle(CommandMessage, "delete", &actionHandle{failure})
	group.Handle(CommandMessage, "create", &actionHandle{delivered})

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

// TestCommandTimestampPassed tests if the command timestamp is passed to the produced event
func TestCommandTimestampPassed(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	var timestamp time.Time

	// Command object to be checked upon
	key, _ := uuid.NewV4()
	command := NewCommand("command", 1, key, []byte("{}"))

	delivered := make(chan interface{}, 1)
	group.HandleFunc(CommandMessage, "command", func(writer ResponseWriter, message interface{}) {
		command := message.(Command)
		timestamp = command.Timestamp

		writer.ProduceEvent("event", 1, uuid.Nil, nil)
	})

	group.HandleFunc(EventMessage, "event", func(writer ResponseWriter, message interface{}) {
		event := message.(Event)
		delivered <- message

		if event.CommandTimestamp.Unix() != timestamp.Unix() {
			t.Fatal("the event timestamp does not match the command timestamp")
		}
	})

	group.ProduceCommand(command)

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-delivered:
	case <-ctx.Done():
		t.Error("the events handle was not called within the deadline")
	}
}

// TestCommandTimestampPassed tests if the command timestamp is passed to the produced event
func TestMessageMarked(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	id := func() uuid.UUID { id, _ := uuid.NewV4(); return id }

	first := NewCommand("command", 1, id(), []byte("{}"))
	second := NewCommand("command", 1, id(), []byte("{}"))

	delivered := make(chan interface{}, 2)

	group.HandleFunc(CommandMessage, "command", func(writer ResponseWriter, message interface{}) {
		time.Sleep(100 * time.Millisecond)
		delivered <- message
	})

	group.ProduceCommand(first)
	group.ProduceCommand(second)

	deadline := time.Now().Add(50 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-ctx.Done():
		if len(delivered) == 2 {
			t.Fatal("the events are unexpectedly consumed")
		}
	}
}
