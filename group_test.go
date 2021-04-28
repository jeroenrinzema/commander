package commander

import (
	"context"
	"testing"
	"time"

	"github.com/jeroenrinzema/commander/dialects/mock"
	"github.com/jeroenrinzema/commander/internal/metadata"
	"github.com/jeroenrinzema/commander/internal/types"
)

type handler struct {
	messages chan interface{}
}

func (handle *handler) Handle(message *Message, writer Writer) {
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

	message := types.NewMessage("testing", 1, nil, nil)

	err := group.ProduceCommand(message)
	if err != nil {
		t.Error(err)
	}
}

// TestProduceEvent tests if able to produce a event
func TestProduceEvent(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	parent := types.NewMessage("parent", 1, nil, nil)
	child := parent.NewMessage("tested", 1, nil, nil)

	err := group.ProduceEvent(child)
	if err != nil {
		t.Error(err)
	}
}

// TestProduceEventNoTopics tests if a error is returned when no topics for production are found
func TestProduceEventNoTopics(t *testing.T) {
	group := NewGroup()

	client, _ := NewClient(group)
	defer client.Close()

	parent := types.NewMessage("parent", 1, nil, nil)
	event := parent.NewMessage("tested", 1, nil, nil)

	err := group.ProduceEvent(event)
	if err != ErrNoTopic {
		t.Fatal("no topic error is not thrown when expected")
	}
}

// TestAsyncCommand tests if plausible to create a async command
func TestAsyncCommand(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	message := types.NewMessage("tested", 1, nil, nil)

	err := group.AsyncCommand(message)
	if err != nil {
		t.Error(err)
	}
}

// TestSyncCommand tests if able to send a sync command
func TestSyncCommand(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	action := "testing"

	message := types.NewMessage(action, 1, nil, nil)
	message.EOS = true

	group.HandleFunc(CommandMessage, action, func(message *Message, writer Writer) {
		_, err := writer.Event(action, 1, nil, nil)
		if err != nil {
			t.Error(err)
		}
	})

	event, err := group.SyncCommand(message)
	event.Ack()

	if err != nil {
		t.Error(err)
	}

	parent, has := metadata.ParentIDFromContext(event.Ctx())
	if !has || parent != metadata.ParentID(message.ID) {
		t.Error("command id and parent do not match")
	}
}

func BenchmarkSyncCommand(b *testing.B) {
	group, client := NewMockClient()
	defer client.Close()

	action := "command"

	message := types.NewMessage(action, 1, nil, nil)
	message.EOS = true

	group.HandleFunc(CommandMessage, action, func(message *Message, writer Writer) {
		_, err := writer.Event(action, 1, nil, nil)
		if err != nil {
			b.Error(err)
		}
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		event, err := group.SyncCommand(message)
		if err != nil {
			b.Error(err)
		}

		parent, has := metadata.ParentIDFromContext(event.Ctx())
		if !has || parent != metadata.ParentID(message.ID) {
			b.Error("command id and parent do not match")
		}

		event.Ack()
	}
}

// TestAwaitEvent tests if plausible to await a event
func TestAwaitEvent(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	timeout := 100 * time.Millisecond
	parent := types.NewMessage("parent", 1, nil, nil)

	go func() {
		event := parent.NewMessage("tested", 1, nil, nil)
		err := group.ProduceEvent(event)
		if err != nil {
			t.Error(err)
		}
	}()

	messages, closer, err := group.NewConsumerWithDeadline(timeout, EventMessage)
	if err != nil {
		t.Fatal(err)
		return
	}

	defer closer()

	message, err := group.AwaitMessage(messages, metadata.ParentID(parent.ID))
	message.Ack()

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
	parent := types.NewMessage(action, 1, nil, nil)

	go func() {
		event := parent.NewMessage(action, 1, nil, nil)
		err := group.ProduceEvent(event)
		if err != nil {
			t.Error(err)
		}
	}()

	messages, closer, err := group.NewConsumerWithDeadline(timeout, EventMessage)
	if err != nil {
		t.Fatal(err)
		return
	}

	defer closer()

	message, err := group.AwaitEventWithAction(messages, metadata.ParentID(parent.ID), action)
	if err != nil {
		t.Error(err)
	}

	message.Ack()
}

// TestAwaitEventIgnoreParent tests if plausible to await a event with action
func TestAwaitEventIgnoreParent(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	timeout := 100 * time.Millisecond
	parent := types.NewMessage("parent", 1, nil, nil)

	messages, closer, err := group.NewConsumerWithDeadline(timeout, EventMessage)
	if err != nil {
		t.Fatal(err)
		return
	}

	go func() {
		event := parent.NewMessage("ignore", 1, nil, nil)
		err := group.ProduceEvent(event)
		if err != nil {
			t.Error(err)
		}
	}()

	defer closer()

	message, err := group.AwaitEventWithAction(messages, metadata.ParentID(parent.ID), "process")
	if err != ErrTimeout {
		t.Error(err)
	}

	message.Ack()
}

// TestEventConsumer tests if events get consumed
func TestEventConsumer(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	events, close, err := group.NewConsumer(EventMessage)
	if err != nil {
		panic(err)
	}

	defer close()

	parent := types.NewMessage("parent", 1, nil, nil)
	event := parent.NewMessage("tested", 1, nil, nil)
	group.ProduceEvent(event)

	deadline := time.Now().Add(500 * time.Millisecond)

	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case event := <-events:
		event.Ack()
	case <-ctx.Done():
		t.Error("no message was consumed within the deadline")
	}
}

// TestCommandConsumer tests if commands get consumed
func TestCommandConsumer(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	messages, close, err := group.NewConsumer(CommandMessage)
	if err != nil {
		panic(err)
	}

	defer close()

	command := types.NewMessage("testing", 1, nil, nil)
	group.ProduceCommand(command)

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case message := <-messages:
		message.Ack()
	case <-ctx.Done():
		t.Error("no message was consumed within the deadline")
	}
}

// TestEventHandleFunc tests if a event handle func get's called
func TestEventHandleFunc(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	action := "testing"
	delivered := make(chan *Message, 1)

	_, err := group.HandleFunc(EventMessage, action, func(message *Message, writer Writer) {
		delivered <- message
	})

	if err != nil {
		t.Error(err)
	}

	event := types.NewMessage(action, 1, nil, nil)
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

	handle := &handler{delivered}
	group.Handle(EventMessage, action, handle)

	event := types.NewMessage(action, 1, nil, nil)
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

	group.Handle(EventMessage, "create", &handler{delivered})
	group.Handle(EventMessage, "delete", &handler{failure})

	event := types.NewMessage("create", 1, nil, nil)
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

	group.Handle(CommandMessage, "delete", &handler{failure})
	group.Handle(CommandMessage, "create", &handler{delivered})

	command := types.NewMessage("create", 1, nil, nil)
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
	command := types.NewMessage("command", 1, nil, nil)

	delivered := make(chan interface{}, 1)
	group.HandleFunc(CommandMessage, "command", func(message *Message, writer Writer) {
		timestamp = message.Timestamp

		writer.Event("event", 1, nil, nil)
	})

	group.HandleFunc(EventMessage, "event", func(message *Message, writer Writer) {
		parent, has := metadata.ParentTimestampFromContext(message.Ctx())
		if !has {
			t.Error("timestamp is not set")
		}

		if time.Time(parent).Unix() != timestamp.Unix() {
			t.Error("the event timestamp does not match the command timestamp")
		}

		delivered <- message
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
	message := types.NewMessage("testing", 1, nil, nil)
	message.Reset()
	go func() {
		message.Ack()
	}()
	message.Finally()
}

// TestNewConsumer tests if able to create a new consumer
func TestNewConsumer(t *testing.T) {
	group, client := NewMockClient()
	defer client.Close()

	var err error

	_, _, err = group.NewConsumer(EventMessage)
	if err != nil {
		t.Error(err)
	}

	unkown := types.MessageType(0)
	_, _, err = group.NewConsumer(unkown)
	if err != ErrNoTopic {
		t.Error("unexpected error", err)
	}
}
