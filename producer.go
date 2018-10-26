package commander

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaProducer is the interface used to produce kafka messages
type KafkaProducer interface {
	Produce(*kafka.Message, chan kafka.Event) error
	Close()
}

// NewProducer initializes a new kafka producer and producer instance.
func NewProducer(config *kafka.ConfigMap) (Producer, error) {
	client, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}

	producer := &producer{
		client: client,
	}

	return producer, nil
}

// Producer interface
type Producer interface {
	// Produce produces a kafka message to the given topic on the configured cluster
	Produce(*kafka.Message) error

	// UseMockProducer replaces the current producer with a mock producer.
	// This method is mainly used for testing purposes
	UseMockProducer() *MockKafkaProducer
}

type producer struct {
	client KafkaProducer
}

func (producer *producer) UseMockProducer() *MockKafkaProducer {
	mock := &MockKafkaProducer{
		events: make(chan kafka.Event),
	}

	producer.client = mock
	return mock
}

func (producer *producer) Produce(msg *kafka.Message) error {
	delivery := make(chan kafka.Event)

	err := producer.client.Produce(msg, delivery)
	if err != nil {
		return err
	}

	event := <-delivery
	message := event.(*kafka.Message)

	if message.TopicPartition.Error != nil {
		return message.TopicPartition.Error
	}

	return nil
}
