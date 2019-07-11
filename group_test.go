package commander

import (
	"context"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander/dialects/mock"
)

type actionHandle struct {
	messages chan interface{}
}

func (handle *actionHandle) Process(writer ResponseWriter, message interface{}) {
	handle.messages <- message
}

// NewMockClient initializes a new in-memory mocking dialect, group and commander client used for testing
func NewMockClient() (*Group, *Client) {
	dialect := mock.NewDialect()
	group := NewGroup(
		NewTopic("events", dialect, EventMessage, DefaultMode),
		NewTopic("commands", dialect, CommandMessage, DefaultMode),
	)

	client, _ := NewClient(group)
	return group, client
}

// TestProduceCommand tests if able to produce a command
func TestProduceCommand(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	key, _ := uuid.NewV4()
	command := NewCommand("testing", 1, key.Bytes(), []byte("{}"))

	group.ProduceCommand(command)
}

// TestProduceEvent tests if able to produce a event
func TestProduceEvent(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	key, _ := uuid.NewV4()
	parent, _ := uuid.NewV4()
	event := NewEvent("tested", 1, parent, key.Bytes(), []byte("{}"))

	group.ProduceEvent(event)
}

// TestProduceEventNoTopics tests if a error is returned when no topics for production are found
func TestProduceEventNoTopics(t *testing.T) {
	group := NewGroup()

	client, _ := NewClient(group)
	defer client.Close()

	key, _ := uuid.NewV4()
	parent, _ := uuid.NewV4()
	event := NewEvent("tested", 1, parent, key.Bytes(), []byte("{}"))

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

	action := "command"
	key, _ := uuid.NewV4()
	command := NewCommand(action, 1, key.Bytes(), nil)
	command.EOS = true

	group.HandleFunc(CommandMessage, action, func(writer ResponseWriter, message interface{}) {
		_, err := writer.ProduceEventEOS(action, 1, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
	})

	event, next, err := group.SyncCommand(command)
	next(nil)

	if err != nil {
		t.Fatal(err)
	}

	if event.Parent != command.ID {
		t.Fatal("command id and parent do not match")
	}
}

func BenchmarkSyncCommand(b *testing.B) {
	group, client := NewMockClient()
	defer client.Close()

	action := "command"
	key, _ := uuid.NewV4()
	command := NewCommand(action, 1, key.Bytes(), nil)
	command.EOS = true

	group.HandleFunc(CommandMessage, action, func(writer ResponseWriter, message interface{}) {
		_, err := writer.ProduceEventEOS(action, 1, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		event, next, err := group.SyncCommand(command)

		if err != nil {
			b.Fatal(err)
		}

		if event.Parent != command.ID {
			b.Fatal("command id and parent do not match")
		}

		next(nil)
	}
}

// TestAwaitEvent tests if plausible to await a event
func TestAwaitEvent(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	timeout := 100 * time.Millisecond
	parent, _ := uuid.NewV4()

	go func() {
		event := NewEvent("tested", 1, parent, nil, nil)
		err := group.ProduceEvent(event)
		if err != nil {
			t.Fatal(err)
		}
	}()

	_, next, err := group.AwaitEvent(timeout, parent, EventMessage)
	next(nil)

	if err != nil {
		t.Fatal(err)
		return
	}
}

// TestAwaitEventAction tests if plausible to await a event action
func TestAwaitEventAction(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	action := "process"
	timeout := 100 * time.Millisecond
	parent, _ := uuid.NewV4()

	go func() {
		event := NewEvent(action, 1, parent, nil, nil)
		err := group.ProduceEvent(event)
		if err != nil {
			t.Fatal(err)
		}
	}()

	_, next, err := group.AwaitEventWithAction(timeout, parent, EventMessage, action)
	next(nil)

	if err != nil {
		t.Error(err)
	}
}

// TestAwaitEventIgnoreParent tests if plausible to await a event and ignore the parent
func TestAwaitEventIgnoreParent(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	timeout := 100 * time.Millisecond
	parent, _ := uuid.NewV4()

	event := NewEvent("ignore", 1, parent, nil, nil)
	err := group.ProduceEvent(event)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = group.AwaitEventWithAction(timeout, parent, EventMessage, "process")
	if err != ErrTimeout {
		t.Error(err)
	}
}

// TestEventConsumer tests if events get consumed
func TestEventConsumer(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	events, next, close, err := group.NewConsumer(EventMessage)
	if err != nil {
		panic(err)
	}

	defer close()

	parent, _ := uuid.NewV4()
	key, _ := uuid.NewV4()

	event := NewEvent("tested", 1, parent, key.Bytes(), []byte("{}"))
	group.ProduceEvent(event)

	deadline := time.Now().Add(500 * time.Millisecond)

	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-events:
	case <-ctx.Done():
		t.Error("no message was consumed within the deadline")
	}

	next(nil)
}

// TestCommandConsumer tests if commands get consumed
func TestCommandConsumer(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	commands, next, close, err := group.NewConsumer(CommandMessage)
	if err != nil {
		panic(err)
	}

	defer close()

	key, _ := uuid.NewV4()
	command := NewCommand("testing", 1, key.Bytes(), []byte("{}"))
	group.ProduceCommand(command)

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-commands:
	case <-ctx.Done():
		t.Error("no message was consumed within the deadline")
	}

	next(nil)
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

	event := NewEvent(action, 1, parent, key.Bytes(), []byte("{}"))
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

	event := NewEvent(action, 1, parent, key.Bytes(), []byte("{}"))
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

	event := NewEvent("create", 1, parent, key.Bytes(), []byte("{}"))
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

	command := NewCommand("create", 1, key.Bytes(), []byte("{}"))
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
	command := NewCommand("command", 1, key.Bytes(), []byte("{}"))

	delivered := make(chan interface{}, 1)
	group.HandleFunc(CommandMessage, "command", func(writer ResponseWriter, message interface{}) {
		command := message.(Command)
		timestamp = command.Timestamp

		writer.ProduceEvent("event", 1, nil, nil)
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

// TestMessageMarked tests if the command timestamp is passed to the produced event
func TestMessageMarked(t *testing.T) {
	group, _ := NewMockClient()
	id := func() []byte { return uuid.Must(uuid.NewV4()).Bytes() }

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

// // TestEventStream tests if able to consume and await a event stream
// func TestEventStream(t *testing.T) {
// 	var event Event

// 	group, client := NewMockClient()
// 	defer client.Close()

// 	group.HandleFunc(CommandMessage, "command", func(writer ResponseWriter, message interface{}) {
// 		writer.ProduceEvent("action", 1, nil, nil)
// 		event, _ = writer.ProduceEventEOS("action", 1, nil, nil)
// 		writer.ProduceEvent("action", 1, nil, nil)
// 	})

// 	command := NewCommand("command", 1, nil, nil)
// 	err := group.ProduceCommand(command)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	timeout := 500 * time.Millisecond
// 	events, marked, erro := group.AwaitEOS(timeout, command.ID, EventMessage)

// 	select {
// 	case err := <-erro:
// 		t.Fatal(err)
// 	case res := <-events:
// 		if res.ID != event.ID {
// 			t.Fatal("returned event is not the expected event")
// 		}

// 		marked <- nil
// 	}
// }
