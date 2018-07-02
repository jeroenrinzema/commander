package commander

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Consumer this consumer consumes messages from a
// kafka topic. A channel is opened to receive kafka messages
type Consumer struct {
	Topic  string
	Events chan kafka.Event

	closing   chan bool
	commander *Commander
}

// Close closes and removes the consumer from the commander instance
func (consumer *Consumer) Close() {
	log.Println("closing consumer")

	if consumer.closing != nil {
		close(consumer.closing)
	}

	for index, con := range consumer.commander.consumers {
		if con == consumer {
			consumer.commander.consumers = append(consumer.commander.consumers[:index], consumer.commander.consumers[index+1:]...)
		}
	}
}

// BeforeClosing creates a new channel that gets called before the consumer gets closed
func (consumer *Consumer) BeforeClosing() chan bool {
	if consumer.closing == nil {
		consumer.closing = make(chan bool)
	}

	return consumer.closing
}
