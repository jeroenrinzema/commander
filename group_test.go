package commander

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

// NewTestGroup initializes a new group used for testing
func NewTestGroup() *Group {
	group := &Group{
		Client:       &client{},
		Timeout:      5 * time.Second,
		EventTopic:   TestTopic,
		CommandTopic: TestTopic,
	}

	return group
}

// NewTestClient initializes a new client used for testing
func NewTestClient(groups ...*Group) (Client, *MockKafkaConsumer, *MockKafkaProducer) {
	config := NewConfig()
	config.AddGroups(groups...)
	config.Group = "testing"

	client, err := New(config)
	if err != nil {
		panic(err)
	}

	consumer := client.Consumer().UseMockConsumer()
	producer := client.Producer().UseMockProducer()

	return client, consumer, producer
}

// TestAsyncCommand tests if plausible to create a async command
func TestAsyncCommand(t *testing.T) {
	group := NewTestGroup()
	command := NewMockCommand()
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

	message := NewMessage(uuid.NewV4().String(), TestTopic, []byte("{}"), []kafka.Header{
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

// TestSyncEvent tests if able to send a sync event
func TestSyncEvent(t *testing.T) {
	group := NewTestGroup()
	NewTestClient(group)

	key := uuid.NewV4()
	parent := uuid.NewV4()
	event := group.NewEvent("testing", 1, parent, key, []byte("{}"))

	group.SyncEvent(event)
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

	message := NewMessage(uuid.NewV4().String(), TestTopic, []byte("{}"), []kafka.Header{
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

// TestEventConsumer test if events get consumed
func TestEventConsumer(t *testing.T) {
	group := NewTestGroup()
	client, consumer, _ := NewTestClient(group)

	go client.Consume()
	go func() {
		deadline := time.Now().Add(500 * time.Millisecond)

		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		events, close := group.NewEventsConsumer()

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

	message := NewMessage(key.String(), group.EventTopic, []byte("{}"), []kafka.Header{
		kafka.Header{
			Key:   ActionHeader,
			Value: []byte("testing"),
		},
		kafka.Header{
			Key:   AcknowledgedHeader,
			Value: []byte("true"),
		},
		kafka.Header{
			Key:   ParentHeader,
			Value: []byte(parent.Bytes()),
		},
		kafka.Header{
			Key:   IDHeader,
			Value: []byte(id.String()),
		},
		kafka.Header{
			Key:   VersionHeader,
			Value: []byte("1"),
		},
	})

	consumer.Emit(message)
}

// TestCommandConsumer test if commands get consumed
func TestCommandConsumer(t *testing.T) {
	group := NewTestGroup()
	client, consumer, _ := NewTestClient(group)

	go client.Consume()
	go func() {
		deadline := time.Now().Add(500 * time.Millisecond)

		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		commands, close := group.NewCommandsConsumer()

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

	message := NewMessage(key.String(), group.CommandTopic, []byte("{}"), []kafka.Header{
		kafka.Header{
			Key:   ActionHeader,
			Value: []byte("testing"),
		},
		kafka.Header{
			Key:   IDHeader,
			Value: []byte(id.String()),
		},
	})

	consumer.Emit(message)
}
