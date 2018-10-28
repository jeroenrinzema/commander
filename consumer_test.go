package commander

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

var (
	// TestTopic kafka testing topic
	TestTopic = Topic{
		Name: "testing",
	}
)

// NewEventMessage creates a new kafka event message with the given values
func NewEventMessage(action string, key uuid.UUID, parent uuid.UUID, id uuid.UUID, version int, topic Topic, value []byte) *kafka.Message {
	message := NewMessage(key, topic, []byte("{}"), []kafka.Header{
		kafka.Header{
			Key:   ActionHeader,
			Value: []byte(action),
		},
		kafka.Header{
			Key:   AcknowledgedHeader,
			Value: []byte("true"),
		},
		kafka.Header{
			Key:   ParentHeader,
			Value: []byte(parent.String()),
		},
		kafka.Header{
			Key:   IDHeader,
			Value: []byte(id.String()),
		},
		kafka.Header{
			Key:   VersionHeader,
			Value: []byte(strconv.Itoa(version)),
		},
	})

	return message
}

// NewCommandMessage creates a new kafka command message with the given values
func NewCommandMessage(action string, key uuid.UUID, id uuid.UUID, topic Topic, value []byte) *kafka.Message {
	message := NewMessage(key, topic, []byte("{}"), []kafka.Header{
		kafka.Header{
			Key:   ActionHeader,
			Value: []byte(action),
		},
		kafka.Header{
			Key:   IDHeader,
			Value: []byte(id.String()),
		},
	})

	return message
}

// NewTestConsumer initializes a new consumer and set's a mock consumer to be used
func NewTestConsumer() (Consumer, *MockKafkaConsumer) {
	config := &kafka.ConfigMap{
		"group.id": "testing",
	}

	consumer, _ := NewConsumer(config)
	mock := consumer.UseMockConsumer()

	go consumer.Consume()
	return consumer, mock
}

// TestNewConsumer tests the constructing of a new consumer start consuming, and close afterwards
func TestNewConsumer(t *testing.T) {
	consumer, _ := NewTestConsumer()
	defer consumer.Close()
}

// TestConsuming test the consuming of messages
func TestConsuming(t *testing.T) {
	consumer, mock := NewTestConsumer()
	messages, _ := consumer.Subscribe(TestTopic)

	wg := sync.WaitGroup{}
	wg.Add(1)

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

	key := uuid.NewV4()
	message := NewMessage(key, TestTopic, []byte("{}"), []kafka.Header{
		kafka.Header{
			Key:   AcknowledgedHeader,
			Value: []byte("true"),
		},
	})

	mock.Emit(message)
	wg.Wait()
}

// TestEvents test if message events are emitted and
// if plausible to manipulate consumed messages.
func TestEvents(t *testing.T) {
	consumer, mock := NewTestConsumer()

	wg := sync.WaitGroup{}
	wg.Add(1)

	// Start a new go routine to test events consumption.
	// If no events are received within the context deadline is a error thrown.
	// Message manipulation is also tested in this method.
	go func() {
		deadline := time.Now().Add(500 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)

		defer cancel()
		defer wg.Done()

		key := "manipulated"
		value := []byte("true")

		delivered := make(chan kafka.Event, 2)

		consumer.OnEvent(BeforeEvent, func(event kafka.Event) {
			switch message := event.(type) {
			case *kafka.Message:
				message.Headers = append(message.Headers, kafka.Header{
					Key:   key,
					Value: value,
				})
			}
		})

		consumer.OnEvent(AfterEvent, func(event kafka.Event) {
			switch message := event.(type) {
			case *kafka.Message:
				manipulated := false

				for _, header := range message.Headers {
					if header.Key == key && string(header.Value) == string(value) {
						manipulated = true
					}
				}

				if !manipulated {
					t.Error("event message has not been manipulated")
				}
			}

			delivered <- event
		})

		select {
		case <-delivered:
		case <-ctx.Done():
			t.Error("no before event was emitted on message consumption")
		}
	}()

	key := uuid.NewV4()
	message := NewMessage(key, TestTopic, []byte("{}"), []kafka.Header{
		kafka.Header{
			Key:   AcknowledgedHeader,
			Value: []byte("true"),
		},
	})

	mock.Emit(message)
	wg.Wait()
}

// BenchmarkConsumer benchmarks the consumption of messages
func BenchmarkConsumer(b *testing.B) {
	config := &kafka.ConfigMap{
		"group.id": "testing",
	}

	consumer, err := NewConsumer(config)
	if err != nil {
		b.Error(err)
	}

	mock := consumer.UseMockConsumer()

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
			key := uuid.NewV4()
			message := NewMessage(key, TestTopic, []byte("{}"), []kafka.Header{
				kafka.Header{
					Key:   AcknowledgedHeader,
					Value: []byte("true"),
				},
			})

			mock.Emit(message)
		}()
	}
}
