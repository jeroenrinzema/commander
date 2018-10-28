package commander

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

// NewMessage creates a new kafka message with the given values
func NewMessage(key uuid.UUID, topic Topic, value []byte, headers []kafka.Header) *kafka.Message {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic.Name,
		},
		Key:       []byte(key.String()),
		Value:     value,
		Timestamp: time.Now(),
		Headers:   headers,
	}

	return message
}

// NewTestProducer initializes a new producer and set's a mock producer to be used
func NewTestProducer() (Producer, *MockKafkaProducer) {
	config := &kafka.ConfigMap{
		"group.id": "testing",
	}

	producer, _ := NewProducer(config)
	mock := producer.UseMockProducer()

	return producer, mock
}

// TestNewProducer tests the constructing of a new producer
func TestNewProducer(t *testing.T) {
	NewTestProducer()
}

// TestProducing tests if able to produce a kafka message
func TestProducing(t *testing.T) {
	producer, _ := NewTestProducer()

	key := uuid.NewV4()
	topic := Topic{
		Name: "testing",
	}

	message := NewMessage(key, topic, []byte("{}"), nil)
	err := producer.Produce(message)

	if err != nil {
		t.Error(err)
	}
}
