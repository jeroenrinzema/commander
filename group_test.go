package commander

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

type testEventActionHandle struct {
	events chan *Event
}

func (a *testEventActionHandle) Handle(event *Event) {
	a.events <- event
}

type testCommandActionHandle struct {
	commands chan *Command
}

func (a *testCommandActionHandle) Handle(writer ResponseWriter, command *Command) {
	a.commands <- command
}

// NewTestGroup initializes a new group used for testing
func NewTestGroup() *Group {
	group := &Group{
		Client:  &client{},
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

// TestNewEvent tests if plausible to create a new event
func TestNewEvent(t *testing.T) {
	group := NewTestGroup()
	parent := uuid.NewV4()
	key := uuid.NewV4()

	group.NewEvent("testing", 1, parent, key, []byte("{}"))
}

// TestNewCommand tests if plausible to create a new command
func TestNewCommand(t *testing.T) {
	group := NewTestGroup()
	key := uuid.NewV4()

	group.NewCommand("testing", key, []byte("{}"))
}

// TestSyncCommand tests if able to send a sync command
func TestSyncCommand(t *testing.T) {
	group := NewTestGroup()
	client, consumer, _ := NewTestClient(group)

	key := uuid.NewV4()
	command := group.NewCommand("testing", key, []byte("{}"))

	go client.Consume()
	go func() {
		_, err := group.SyncCommand(command)
		if err != nil {
			t.Error(err)
		}
	}()

	message := NewMessage(key, TestTopic, []byte("{}"), []kafka.Header{
		kafka.Header{
			Key:   AcknowledgedHeader,
			Value: []byte("true"),
		},
		kafka.Header{
			Key:   ParentHeader,
			Value: command.ID.Bytes(),
		},
	})

	consumer.Emit(message)
}

// TestAwaitEvent tests if plausible to await a event
func TestAwaitEvent(t *testing.T) {
	group := NewTestGroup()
	client, consumer, _ := NewTestClient(group)

	go client.Consume()

	timeout := 2 * time.Second
	parent := uuid.NewV4()

	go func() {
		_, err := group.AwaitEvent(timeout, parent)
		if err != nil {
			t.Error(err)
		}
	}()

	key := uuid.NewV4()
	message := NewMessage(key, TestTopic, []byte("{}"), []kafka.Header{
		kafka.Header{
			Key:   AcknowledgedHeader,
			Value: []byte("true"),
		},
		kafka.Header{
			Key:   ParentHeader,
			Value: parent.Bytes(),
		},
	})

	consumer.Emit(message)
}

// TestProduceCommand tests if able to produce a command
func TestProduceCommand(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	key := uuid.NewV4()
	command := group.NewCommand("testing", key, []byte("{}"))

	group.ProduceCommand(command)
}

// TestProduceEvent tests if able to produce a event
func TestProduceEvent(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	key := uuid.NewV4()
	parent := uuid.NewV4()
	event := group.NewEvent("testing", 1, parent, key, []byte("{}"))

	group.ProduceEvent(event)
}

// TestEventConsumer tests if events get consumed
func TestEventConsumer(t *testing.T) {
	group := NewTestGroup()
	client, consumer, _ := NewTestClient(group)

	go client.Consume()
	go func() {
		deadline := time.Now().Add(500 * time.Millisecond)

		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		events, close := group.NewEventConsumer()

		defer cancel()
		defer close()

		select {
		case <-events:
		case <-ctx.Done():
			t.Error("no message was consumed within the deadline")
		}
	}()

	id := uuid.NewV4()
	parent := uuid.NewV4()
	key := uuid.NewV4()

	message := NewEventMessage("testing", key, parent, id, 1, group.Topics, []byte("{}"))
	consumer.Emit(message)
}

// TestCommandConsumer tests if commands get consumed
func TestCommandConsumer(t *testing.T) {
	group := NewTestGroup()
	client, consumer, _ := NewTestClient(group)

	go client.Consume()
	go func() {
		deadline := time.Now().Add(500 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)

		commands, close := group.NewCommandConsumer()

		defer cancel()
		defer close()

		select {
		case <-commands:
		case <-ctx.Done():
			t.Error("no message was consumed within the deadline")
		}
	}()

	id := uuid.NewV4()
	key := uuid.NewV4()

	message := NewCommandMessage("testing", key, id, group.Topics, []byte("{}"))
	consumer.Emit(message)
}

// TestEventHandleFunc tests if a event handle func get's called
func TestEventHandleFunc(t *testing.T) {
	group := NewTestGroup()
	client, consumer, _ := NewTestClient(group)

	go client.Consume()

	action := "testing"
	version := 1
	delivered := make(chan *Event, 1)

	group.EventHandleFunc(action, []int{version}, func(event *Event) {
		delivered <- event
	})

	id := uuid.NewV4()
	parent := uuid.NewV4()
	key := uuid.NewV4()

	message := NewEventMessage(action, key, parent, id, version, group.Topics, []byte("{}"))
	consumer.Emit(message)

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
	client, consumer, _ := NewTestClient(group)

	go client.Consume()

	action := "testing"
	version := 1
	delivered := make(chan *Event, 1)

	handle := &testEventActionHandle{delivered}
	group.EventHandle(action, []int{version}, handle)

	id := uuid.NewV4()
	parent := uuid.NewV4()
	key := uuid.NewV4()

	message := NewEventMessage(action, key, parent, id, version, group.Topics, []byte("{}"))
	consumer.Emit(message)

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-delivered:
	case <-ctx.Done():
		t.Error("the events handle was not called within the deadline")
	}
}

// TestMultipleEventVersionsHandle tests if multiple event versions get handled
func TestMultipleEventVersionsHandle(t *testing.T) {
	group := NewTestGroup()
	client, consumer, _ := NewTestClient(group)

	go client.Consume()

	action := "testing"
	versions := []int{1, 2}
	delivered := make(chan *Event, len(versions))

	group.EventHandleFunc(action, versions, func(event *Event) {
		delivered <- event
	})

	id := uuid.NewV4()
	parent := uuid.NewV4()
	key := uuid.NewV4()

	// Emit messages for the given versions
	for _, version := range versions {
		message := NewEventMessage(action, key, parent, id, version, group.Topics, []byte("{}"))
		consumer.Emit(message)

		deadline := time.Now().Add(500 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)

		select {
		case <-delivered:
		case <-ctx.Done():
			t.Error("the events handle was not called within the deadline")
		}

		cancel()
	}
}

// TestIgnoreMultipleEventVersionsHandle tests if certain versions get ignored by the event handle
func TestIgnoreMultipleEventVersionsHandle(t *testing.T) {
	group := NewTestGroup()
	client, consumer, _ := NewTestClient(group)

	go client.Consume()

	action := "testing"
	versions := []int{1, 2}
	handled := 1
	delivered := make(chan *Event, len(versions))

	group.EventHandleFunc(action, []int{handled}, func(event *Event) {
		delivered <- event
	})

	id := uuid.NewV4()
	parent := uuid.NewV4()
	key := uuid.NewV4()

	// Emit messages for the given versions
	for _, version := range versions {
		message := NewEventMessage(action, key, parent, id, version, group.Topics, []byte("{}"))
		consumer.Emit(message)

		deadline := time.Now().Add(500 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)

		received := false

		select {
		case <-delivered:
			received = true
		case <-ctx.Done():
		}

		if received && version != handled {
			t.Error("the events handle received a event of a version it did not expect")
		}

		cancel()
	}
}

// TestCommandHandleFunc tests if a command handle func get's called
func TestCommandHandleFunc(t *testing.T) {
	group := NewTestGroup()
	client, consumer, _ := NewTestClient(group)

	go client.Consume()

	action := "testing"
	delivered := make(chan *Command, 1)

	group.CommandHandleFunc(action, func(writer ResponseWriter, command *Command) {
		delivered <- command
	})

	id := uuid.NewV4()
	key := uuid.NewV4()

	message := NewCommandMessage(action, key, id, group.Topics, []byte("{}"))
	consumer.Emit(message)

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-delivered:
	case <-ctx.Done():
		t.Error("the command handle was not called within the deadline")
	}
}

// TestCommandHandle tests if a command handle get's called
func TestCommandHandle(t *testing.T) {
	group := NewTestGroup()
	client, consumer, _ := NewTestClient(group)

	go client.Consume()

	action := "testing"
	delivered := make(chan *Command, 1)

	handle := &testCommandActionHandle{delivered}
	group.CommandHandle(action, handle)

	id := uuid.NewV4()
	key := uuid.NewV4()

	message := NewCommandMessage(action, key, id, group.Topics, []byte("{}"))
	consumer.Emit(message)

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-delivered:
	case <-ctx.Done():
		t.Error("the command handle was not called within the deadline")
	}
}
