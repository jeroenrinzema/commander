package commander

import (
	"github.com/Shopify/sarama"
)

// Consumer this consumer consumes messages from a
// kafka topic. A channel is opened to receive kafka messages
type Consumer struct {
	Topic      string
	Partitions []int32
	Offset     int64

	messages      chan *sarama.ConsumerMessage
	closing       chan bool
	consumers     []sarama.PartitionConsumer
	consumer      sarama.Consumer
	subscriptions []*ConsumerSubscription
}

type ConsumerSubscription struct {
	messages chan *sarama.ConsumerMessage
}

func (consumer *Consumer) Consume(saramaConsumer sarama.Consumer) {
	consumer.consumer = saramaConsumer
	consumer.messages = make(chan *sarama.ConsumerMessage)

	for _, partition := range consumer.Partitions {
		pc, err := consumer.consumer.ConsumePartition(consumer.Topic, partition, consumer.Offset)

		if err != nil {
			continue
		}

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				consumer.messages <- message
			}
		}(pc)

		consumer.consumers = append(consumer.consumers, pc)
	}

	for message := range consumer.messages {
		for _, subscriber := range consumer.subscriptions {
			subscriber.messages <- message
		}
	}
}

func (consumer *Consumer) Close() {
	for _, subscription := range consumer.subscriptions {
		consumer.UnSubscribe(subscription)
	}
}

func (consumer *Consumer) Subscribe() *ConsumerSubscription {
	subscription := &ConsumerSubscription{
		messages: make(chan *sarama.ConsumerMessage),
	}

	consumer.subscriptions = append(consumer.subscriptions, subscription)
	return subscription
}

func (consumer *Consumer) UnSubscribe(sub *ConsumerSubscription) {
	for index, subscription := range consumer.subscriptions {
		if subscription == sub {
			consumer.subscriptions = append(consumer.subscriptions[:index], consumer.subscriptions[index+1:]...)
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
