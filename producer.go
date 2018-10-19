package commander

import "github.com/confluentinc/confluent-kafka-go/kafka"

// KafkaProducer is the interface used to produce kafka messages
type KafkaProducer interface {
	Close()
	Event() chan kafka.Event
	Produce(*kafka.Message, chan kafka.Event) error
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
}

type producer struct {
	client *kafka.Producer
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
