package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander"
)

// Dialect represents the kafka dialect
type Dialect struct {
	Groups []commander.Group

	consumer *Consumer
	producer *Producer
}

// Open opens a kafka consumer and producer
func (dialect *Dialect) Open(connectionstring string, groups ...*commander.Group) (commander.Consumer, commander.Producer, error) {
	values := ParseConnectionstring(connectionstring)
	err := ValidateConnectionKeyVal(values)
	if err != nil {
		return nil, nil, err
	}

	connection, err := NewConfig(values)
	if err != nil {
		return nil, nil, err
	}

	config := sarama.NewConfig()
	config.Version = connection.Version
	config.Producer.Return.Successes = true

	consumerGroup, err := sarama.NewConsumerGroup(connection.Brokers, connection.Group, config)
	if err != nil {
		return nil, nil, err
	}

	asyncProducer, err := sarama.NewAsyncProducer(connection.Brokers, config)
	if err != nil {
		return nil, nil, err
	}

	consumer := NewConsumer(consumerGroup, groups...)
	producer := NewProducer(asyncProducer)

	dialect.consumer = consumer
	dialect.producer = producer

	return consumer, producer, nil
}

// Close closes the Kafka consumers and producers
func (dialect *Dialect) Close() error {
	var err error

	err = dialect.consumer.Close()
	if err != nil {
		return err
	}

	err = dialect.producer.Close()
	if err != nil {
		return err
	}

	return nil
}

// Healthy returns a boolean that reprisents if the dialect is healthy
func (dialect *Dialect) Healthy() bool {
	return true
}
