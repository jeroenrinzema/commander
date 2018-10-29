package mocks

import "github.com/jeroenrinzema/commander"

// NewMockClient initializes a new mock client
func NewMockClient(config commander.Config) (commander.Client, *KafkaProducer, *KafkaProducer) {
	client, err := commander.New(config)
	if err != nil {
		panic(err)
	}

	consumer := client.Consumer().UseMockConsumer()
	producer := client.Producer().UseMockProducer()

	return client, consumer, producer
}
