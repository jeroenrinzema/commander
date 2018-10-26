package commander

import (
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
