package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander"
)

// NewProducer constructs a new producer
func NewProducer(client sarama.SyncProducer) *Producer {
	producer := &Producer{
		client: client,
	}

	return producer
}

// Producer produces kafka messages
type Producer struct {
	client     sarama.SyncProducer
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

	_, _, err := producer.client.SendMessage(m)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the kafka producer
func (producer *Producer) Close() error {
	producer.client.Close()
	producer.production.Wait()

	return nil
}
