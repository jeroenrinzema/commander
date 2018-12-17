package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander"
)

// NewProducer constructs a new producer
func NewProducer(client sarama.AsyncProducer) *Producer {
	producer := &Producer{
		client: client,
	}

	return producer
}

// Producer produces kafka messages
type Producer struct {
	client     sarama.AsyncProducer
	production sync.WaitGroup
}

// Publish publishes the given message
func (producer *Producer) Publish(message *commander.Message) error {
	producer.production.Add(1)
	defer producer.production.Done()

	headers := []sarama.RecordHeader{}
	for _, header := range message.Headers {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(header.Key),
			Value: header.Value,
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
