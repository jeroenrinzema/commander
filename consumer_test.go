package commander

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

var (
	TestGroup  = "testing"
	TestTopic  = "testing"
	TestTopics = []string{TestTopic}
)

func NewMessage(key, topic string, value []byte, headers []kafka.Header) *kafka.Message {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic,
		},
		Key:       []byte(key),
		Value:     value,
		Timestamp: time.Now(),
		Headers:   headers,
	}

	return message
}

// NewMockConsumer constructs a new MockConsumer
func NewMockConsumer() *MockConsumer {
	consumer := &MockConsumer{
		events: make(chan kafka.Event),
	}

	return consumer
}

// MockConsumer mocks a Sarama cluster consumer.
// Message consumption could be simulated by emitting messages via the Emit method.
type MockConsumer struct {
	events chan kafka.Event
}

func (mock *MockConsumer) Emit(event kafka.Event)   { mock.events <- event }
func (mock *MockConsumer) Events() chan kafka.Event { return mock.events }

func (mock *MockConsumer) SubscribeTopics([]string, kafka.RebalanceCb) error { return nil }
func (mock *MockConsumer) Assign([]kafka.TopicPartition) error               { return nil }
func (mock *MockConsumer) Unassign() error                                   { return nil }
func (mock *MockConsumer) Close() error                                      { return nil }

// TestNewConsumer tests the constructing of a new consumer start consuming, and close afterwards
func TestNewConsumer(t *testing.T) {
	config := &kafka.ConfigMap{
		"group.id": TestGroup,
	}

	consumer, err := NewConsumer(config)
	if err != nil {
		t.Error(err)
	}

	consumer.kafka = NewMockConsumer()
	defer consumer.Close()
	go consumer.Consume()
}

// TestConsuming test the consuming of messages
func TestConsuming(t *testing.T) {
	config := &kafka.ConfigMap{
		"group.id": TestGroup,
	}

	consumer, err := NewConsumer(config)
	if err != nil {
		t.Error(err)
	}

	cluster := NewMockConsumer()
	consumer.kafka = cluster

	defer consumer.Close()
	go consumer.Consume()

	wg := sync.WaitGroup{}
	wg.Add(1)

	messages, _ := consumer.Subscribe(TestTopic)

	// Start a new go routine to test message consumption.
	// If no message is received within the context deadline is a error thrown.
	go func() {
		deadline := time.Now().Add(500 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)

		defer wg.Done()
		defer cancel()

		select {
		case <-messages:
		case <-ctx.Done():
			t.Error("no message was consumed within the deadline")
		}
	}()

	message := NewMessage(uuid.NewV4().String(), TestTopic, []byte("{}"), []kafka.Header{
		kafka.Header{
			Key:   "acknowledged",
			Value: []byte("true"),
		},
	})

	cluster.Emit(message)
	wg.Wait()
}

// TestEvents test if message events are emitted and
// if plausible to manipulate consumed messages.
func TestEvents(t *testing.T) {
	config := &kafka.ConfigMap{
		"group.id": TestGroup,
	}

	consumer, err := NewConsumer(config)
	if err != nil {
		t.Error(err)
	}

	cluster := NewMockConsumer()
	consumer.kafka = cluster

	wg := sync.WaitGroup{}
	wg.Add(1)

	go consumer.Consume()

	before, _ := consumer.OnEvent(BeforeEvent)
	after, _ := consumer.OnEvent(AfterEvent)

	// Start a new go routine to test events consumption.
	// If no events are received within the context deadline is a error thrown.
	// Message manipulation is also tested in this method.
	go func() {
		deadline := time.Now().Add(500 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)

		defer cancel()
		defer wg.Done()

		manipulatedKey := "manipulated"
		manipulatedValue := []byte("true")

		select {
		case event := <-before:
			switch message := event.(type) {
			case *kafka.Message:
				message.Headers = append(message.Headers, kafka.Header{
					Key:   manipulatedKey,
					Value: manipulatedValue,
				})
			}
		case <-ctx.Done():
			t.Error("no before event was emitted on message consumption")
		}

		select {
		case event := <-after:
			switch message := event.(type) {
			case *kafka.Message:
				manipulated := false

				for _, header := range message.Headers {
					if header.Key == manipulatedKey && string(header.Value) == string(manipulatedValue) {
						manipulated = true
					}
				}

				if !manipulated {
					t.Error("event message has not been manipulated")
				}
			}
		case <-ctx.Done():
			t.Error("no after event was emitted on message consumption")
		}
	}()

	message := NewMessage(uuid.NewV4().String(), TestTopic, []byte("{}"), []kafka.Header{
		kafka.Header{
			Key:   "acknowledged",
			Value: []byte("true"),
		},
	})

	cluster.Emit(message)
	wg.Wait()
}

func TestClosing(t *testing.T) {
	config := &kafka.ConfigMap{
		"group.id": TestGroup,
	}

	consumer, err := NewConsumer(config)
	if err != nil {
		t.Error(err)
	}

	_, closeEvent := consumer.OnEvent(BeforeEvent)
	_, closeSub := consumer.Subscribe(TestTopic)

	closeEvent()
	closeSub()

	if len(consumer.events[BeforeEvent]) != 0 {
		t.Error("The consumer event did not close correctly")
	}

	if len(consumer.Topics[TestTopic]) != 0 {
		t.Error("The consumer subscription did not close correctly")
	}

	consumer.Subscribe(TestTopic)
	consumer.OnEvent(AfterEvent)

	consumer.Close()

	if len(consumer.events[BeforeEvent]) != 0 {
		t.Error("The consumer event did not close correctly after the consumer closed")
	}

	if len(consumer.Topics[TestTopic]) != 0 {
		t.Error("The consumer subscription did not close correctly after the consumer closed")
	}
}

func BenchmarkConsumer(b *testing.B) {
	config := &kafka.ConfigMap{
		"group.id": TestGroup,
	}

	consumer, err := NewConsumer(config)
	if err != nil {
		b.Error(err)
	}

	cluster := NewMockConsumer()
	consumer.kafka = cluster

	defer consumer.Close()
	go consumer.Consume()

	wg := sync.WaitGroup{}
	wg.Add(b.N)

	messages, _ := consumer.Subscribe(TestTopic)

	go func() {
		for range messages {
			wg.Done()
		}
	}()

	for n := 0; n < b.N; n++ {
		go func() {
			message := NewMessage(uuid.NewV4().String(), TestTopic, []byte("{}"), []kafka.Header{
				kafka.Header{
					Key:   "acknowledged",
					Value: []byte("true"),
				},
			})

			cluster.Emit(message)
		}()
	}
}
