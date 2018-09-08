package commander

import (
	"sync"

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
	mutex         sync.Mutex

	before []*EventSubscription
	after  []*EventSubscription
}

// EventSubscription is a struct the contains all info of a event subscription.
// A event subscription is created when a users wants to preform actions before or after a message is consumed.
type EventSubscription struct {
	closing  chan bool
	messages chan *sarama.ConsumerMessage
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
		for _, subscription := range consumer.before {
			subscription.messages <- message
		}

		consumer.mutex.Lock()

		for _, subscriber := range consumer.subscriptions {
			if message.Topic != subscriber.Topic {
				continue
			}

			subscriber.messages <- message
		}

		cluster.MarkOffset(message, "")
		consumer.mutex.Unlock()

		for _, subscription := range consumer.after {
			subscription.messages <- message
		}
	}

	return nil
}

// Close closes the consumer and all it's subscriptions
func (consumer *Consumer) Close() {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for _, subscription := range consumer.subscriptions {
		consumer.UnSubscribe(subscription)
	}
}

// Subscribe creates a new consumer subscription that will start to receive
// messages consumed by the consumer of the given topic.
func (consumer *Consumer) Subscribe(topic string) *ConsumerSubscription {
	subscription := &ConsumerSubscription{
		Topic:    topic,
		closing:  make(chan bool, 1),
		messages: make(chan *sarama.ConsumerMessage, 1),
	}

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	consumer.subscriptions = append(consumer.subscriptions, subscription)
	return subscription
}

// UnSubscribe unsubscribes the given subscription from the consumer
func (consumer *Consumer) UnSubscribe(sub *ConsumerSubscription) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for index, subscription := range consumer.subscriptions {
		if subscription == sub {
			close(subscription.closing)
			consumer.subscriptions = append(consumer.subscriptions[:index], consumer.subscriptions[index+1:]...)
			break
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

// CreateEventSubscription creates and retunes a new EventSubscription instance
func (consumer *Consumer) CreateEventSubscription() *EventSubscription {
	subscription := &EventSubscription{
		messages: make(chan *sarama.ConsumerMessage),
		closing:  make(chan bool, 1),
	}

	return subscription
}

// BeforeConsuming returns a channel that will receive messages consumed by the consumer.
// The returned channel has no buffer to prevent deadlocks.
func (consumer *Consumer) BeforeConsuming() *EventSubscription {
	subscription := consumer.CreateEventSubscription()
	consumer.before = append(consumer.before, subscription)
	return subscription
}

// AfterConsumed returns a channel that will receive messages consumed by the consumer.
// The returned channel has no buffer to prevent deadlocks.
func (consumer *Consumer) AfterConsumed() *EventSubscription {
	subscription := consumer.CreateEventSubscription()
	consumer.after = append(consumer.after, subscription)
	return subscription
}
