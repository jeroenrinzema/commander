package producer

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander"
	"github.com/jeroenrinzema/commander/dialects/kafka/metadata"
)

// NewClient constructs a new producer client
func NewClient() *Client {
	client := &Client{}
	return client
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
func (client *Client) Publish(produce *commander.Message) error {
	client.production.Add(1)
	defer client.production.Done()

	message := metadata.MessageToMessage(produce)
	client.producer.Input() <- message

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
