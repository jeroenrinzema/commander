package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander"
)

// NewProducer constructs a new producer
func NewProducer(brokers []string, config *sarama.Config) (*Producer, error) {
	producer := &Producer{}
	err := producer.Connect(brokers, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

// Producer produces kafka messages
type Producer struct {
	client     sarama.AsyncProducer
	brokers    []string
	config     *sarama.Config
	production sync.WaitGroup
}

// Connect initializes and opens a new Sarama producer group.
func (producer *Producer) Connect(brokers []string, config *sarama.Config) error {
	client, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return err
	}

	producer.client = client
	producer.brokers = brokers
	producer.config = config

	return nil
}

// Publish publishes the given message
func (producer *Producer) Publish(message *commander.Message) error {
	producer.production.Add(1)
	defer producer.production.Done()

	headers := []sarama.RecordHeader{}
	for key, value := range message.Headers {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(value),
		})
	}

	m := &sarama.ProducerMessage{
		Topic:   message.Topic.Name,
		Key:     sarama.ByteEncoder(message.Key),
		Value:   sarama.ByteEncoder(message.Value),
		Headers: headers,
	}

	producer.client.Input() <- m

	select {
	case err := <-producer.client.Errors():
		return err
	case <-producer.client.Successes():
		// Continue
	}

	return nil
}

// Close closes the kafka producer
func (producer *Producer) Close() error {
	producer.client.Close()
	producer.production.Wait()

	return nil
}
