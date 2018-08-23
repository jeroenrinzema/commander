package commander

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// Consumer this consumer consumes messages from a
// kafka topic. A channel is opened to receive kafka messages
type Consumer struct {
	Topics []string
	Group  string

	closing       chan bool
	consumers     []sarama.PartitionConsumer
	subscriptions []*ConsumerSubscription
	cluster       *cluster.Consumer
}

// ConsumerSubscription is a struct that contains all info of a consumer subscription.
// A subscription needs to be created in order to consume the messages received on the consumer.
type ConsumerSubscription struct {
	Topic    string
	closing  chan bool
	messages chan *sarama.ConsumerMessage
}

// Consume starts consuming the given topics with the given consumer group.
func (consumer *Consumer) Consume(client *cluster.Client) error {
	cluster, err := cluster.NewConsumerFromClient(client, consumer.Group, consumer.Topics)

	if err != nil {
		return err
	}

	consumer.cluster = cluster

	for message := range consumer.cluster.Messages() {
		for _, subscriber := range consumer.subscriptions {
			if message.Topic != subscriber.Topic {
				continue
			}

			subscriber.messages <- message
		}

		cluster.MarkOffset(message, "")
	}

	return nil
}

// Close closes the consumer and all it's subscriptions
func (consumer *Consumer) Close() {
	for _, subscription := range consumer.subscriptions {
		consumer.UnSubscribe(subscription)
	}
}

// Subscribe creates a new consumer subscription that will start to receive
// messages consumed by the consumer.
func (consumer *Consumer) Subscribe(topic string) *ConsumerSubscription {
	subscription := &ConsumerSubscription{
		Topic:    topic,
		closing:  make(chan bool, 1),
		messages: make(chan *sarama.ConsumerMessage, 1),
	}

	consumer.subscriptions = append(consumer.subscriptions, subscription)
	return subscription
}

// UnSubscribe unsubscribes the given subscription from the consumer
func (consumer *Consumer) UnSubscribe(sub *ConsumerSubscription) {
	for index, subscription := range consumer.subscriptions {
		if subscription == sub {
			close(subscription.closing)
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
