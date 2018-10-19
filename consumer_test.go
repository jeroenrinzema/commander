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
	// TestGroup kafka testing group
	TestGroup = "testing"
	// TestTopic kafka testing topic
	TestTopic = Topic{
		Name: "testing",
	}
)

// NewMessage creates a new kafka message with the given values
func NewMessage(key string, topic Topic, value []byte, headers []kafka.Header) *kafka.Message {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic.Name,
		},
		Key:       []byte(key),
		Value:     value,
		Timestamp: time.Now(),
		Headers:   headers,
	}

	return message
}

// NewMockConsumer initializes a new consumer and set's a mock consumer to be used
func NewMockConsumer() (Consumer, *MockKafkaConsumer) {
	config := &kafka.ConfigMap{
		"group.id": TestGroup,
	}

	consumer, _ := NewConsumer(config)
	mock := consumer.UseMockConsumer()

	defer consumer.Close()
	go consumer.Consume()

	return consumer, mock
}

// TestNewConsumer tests the constructing of a new consumer start consuming, and close afterwards
func TestNewConsumer(t *testing.T) {
	NewMockConsumer()
}

// TestConsuming test the consuming of messages
func TestConsuming(t *testing.T) {
	consumer, mock := NewMockConsumer()
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

	message := NewMessage(uuid.NewV4().String(), TestTopic, []byte("{}"), []kafka.Header{
		kafka.Header{
			Key:   "acknowledged",
			Value: []byte("true"),
		},
	})

	mock.Emit(message)
	wg.Wait()
}

// TestEvents test if message events are emitted and
// if plausible to manipulate consumed messages.
func TestEvents(t *testing.T) {
	consumer, mock := NewMockConsumer()

	before, _ := consumer.OnEvent(BeforeEvent)
	after, _ := consumer.OnEvent(AfterEvent)

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

		select {
		case event := <-before:
			switch message := event.(type) {
			case *kafka.Message:
				message.Headers = append(message.Headers, kafka.Header{
					Key:   key,
					Value: value,
				})
			}
		case event := <-after:
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
		case <-ctx.Done():
			t.Error("no before event was emitted on message consumption")
		}
	}()

	message := NewMessage(uuid.NewV4().String(), TestTopic, []byte("{}"), []kafka.Header{
		kafka.Header{
			Key:   "acknowledged",
			Value: []byte("true"),
		},
	})

	mock.Emit(message)
	wg.Wait()
}

// BenchmarkConsumer benchmarks the consumption of messages
func BenchmarkConsumer(b *testing.B) {
	config := &kafka.ConfigMap{
		"group.id": TestGroup,
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
			message := NewMessage(uuid.NewV4().String(), TestTopic, []byte("{}"), []kafka.Header{
				kafka.Header{
					Key:   "acknowledged",
					Value: []byte("true"),
				},
			})

			mock.Emit(message)
		}()
	}
}
