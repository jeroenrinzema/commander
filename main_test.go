package commander

import (
	"context"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
)

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

// TestClosingConsumptions test if consumptions get closed properly
func TestClosingConsumptions(t *testing.T) {
	group := NewTestGroup()
	client, consumer, _ := NewTestClient(group)

	go client.Consume()

	action := "testing"
	version := 1
	delivered := make(chan *Event, 1)

	group.EventHandleFunc(action, []int{version}, func(event *Event) {
		time.Sleep(1 * time.Second)
		delivered <- event
	})

	id := uuid.NewV4()
	parent := uuid.NewV4()
	key := uuid.NewV4()

	message := NewEventMessage(action, key, parent, id, version, group.Topics, []byte("{}"))

	consumer.Emit(message)
	client.Close()

	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	defer cancel()

	select {
	case <-ctx.Done():
	case <-delivered:
		t.Error("the client did not close safely")
	}
}
