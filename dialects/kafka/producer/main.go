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
	producer   sarama.SyncProducer
	conn       sarama.Client
	production sync.WaitGroup
}

// Healthy checks the health of the Kafka client
func (client *Client) Healthy() bool {
	return len(client.conn.Brokers()) == 0
}

// Connect initializes and opens a new Sarama producer group.
func (client *Client) Connect(brokers []string, config *sarama.Config) error {
	conn, err := sarama.NewClient(brokers, config)
	if err != nil {
		return err
	}

	producer, err := sarama.NewSyncProducerFromClient(conn)
	if err != nil {
		return err
	}

	client.producer = producer
	client.conn = conn

	return nil
}

// Publish publishes the given message
func (client *Client) Publish(produce *commander.Message) error {
	client.production.Add(1)
	defer client.production.Done()

	message := metadata.MessageToMessage(produce)
	_, _, err := client.producer.SendMessage(message)
	return err
}

// Close closes the Kafka client producer
func (client *Client) Close() error {
	client.producer.Close()
	client.production.Wait()

	return nil
}
