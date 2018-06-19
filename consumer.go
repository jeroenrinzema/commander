package commander

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var topics = []string{}
var consumers = []*Consumer{}

// SubscribeTopics subscribe to the stored topics
func SubscribeTopics(commander *Commander) {
	fmt.Println("Subscribing to topics:", topics)
	commander.Consumer.SubscribeTopics(topics, nil)
}

// Consumer this consumer consumes messages from a
// kafka topic. A channel is opened to receive kafka messages
type Consumer struct {
	Commander *Commander
	Topic     string
	Messages  chan *kafka.Message
}

// Read start reading/awaiting messages from the assigned kafka topic
func (c *Consumer) Read() {
	consumers = append(consumers, c)

	// Add consumer topic to topic slice if not exists
	for _, topic := range topics {
		if topic == c.Topic {
			return
		}
	}

	topics = append(topics, c.Topic)
	SubscribeTopics(c.Commander)
}

// Close stop reading/awaiting messages and remove the consumer from the list
func (c *Consumer) Close() {
	close(c.Messages)

	for index, consumer := range consumers {
		if c == consumer {
			consumers = append(consumers[:index], consumers[index+1:]...)
		}
	}
}
