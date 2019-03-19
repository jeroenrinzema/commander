package producer

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander"
)

// NewClient constructs a new producer client
func NewClient(brokers []string, config *sarama.Config) (*Client, error) {
	client := &Client{}
	err := client.Connect(brokers, config)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// Client produces kafka messages
type Client struct {
	producer   sarama.AsyncProducer
	brokers    []string
	config     *sarama.Config
	production sync.WaitGroup
}

// Connect initializes and opens a new Sarama producer group.
func (client *Client) Connect(brokers []string, config *sarama.Config) error {
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return err
	}

	client.producer = producer
	client.brokers = brokers
	client.config = config

	return nil
}

// Publish publishes the given message
func (client *Client) Publish(message *commander.Message) error {
	client.production.Add(1)
	defer client.production.Done()

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

	client.producer.Input() <- m

	select {
	case err := <-client.producer.Errors():
		return err
	case <-client.producer.Successes():
		// Continue
	}

	return nil
}

// Close closes the Kafka client producer
func (client *Client) Close() error {
	client.producer.Close()
	client.production.Wait()

	return nil
}
