package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander"
)

// NewClient constructs a new client from the given config
func NewClient(connection Config) (sarama.Client, error) {
	config := sarama.NewConfig()
	config.Version = connection.Version

	client, err := sarama.NewClient(connection.Brokers, config)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// Dialect represents the kafka dialect
type Dialect struct {
	Groups []commander.Group
}

// Open opens a kafka consumer and producer
func (dialect *Dialect) Open(connectionstring string, groups ...*commander.Group) (commander.Consumer, commander.Producer, error) {
	values := ParseConnectionstring(connectionstring)
	err := ValidateConnectionKeyVal(values)
	if err != nil {
		return nil, nil, err
	}

	config, err := NewConfig(values)
	if err != nil {
		return nil, nil, err
	}

	client, err := NewClient(config)
	if err != nil {
		return nil, nil, err
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(config.Group, client)
	if err != nil {
		return nil, nil, err
	}

	asyncProducer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, nil, err
	}

	consumer := &Consumer{client: consumerGroup}
	producer := &Producer{client: asyncProducer}

	return consumer, producer, nil
}

// Healthy returns a boolean that reprisents if the dialect is healthy
func (dialect *Dialect) Healthy() bool {
	return true
}
