package commander

import (
	"sync"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// NewConsumer initalizes a new consumer struct with the given cluster client.
func NewConsumer(client *cluster.Client, group string) *Consumer {
	consumer := &Consumer{
		Group:  group,
		client: client,
	}

	return consumer
}

// Consumer this consumer consumes messages from a
// kafka topic. A channel is opened to receive kafka messages
type Consumer struct {
	Topics map[string][]chan *sarama.ConsumerMessage
	Group  string

	closing   chan bool
	consumers []sarama.PartitionConsumer
	client    *cluster.Client
	cluster   *cluster.Consumer
	mutex     sync.Mutex
	events    map[string][]chan *sarama.ConsumerMessage
}

// AddTopic adds the given topic to the subscribed topics map.
// Topics can only be added before consuming.
func (consumer *Consumer) AddTopic(topic string) {
	consumer.Topics[topic] = []chan *sarama.ConsumerMessage{}
}

// Consume subscribes to all given topics and creates a new consumer.
// The consumed messages get send to subscribed topic subscriptions.
// A topic subscription could be made with the consumer.Subscribe method.
// Before a message is passed on to any topic subscription is the BeforeEvent event emitted.
// And once a event has been consumed and processed is the AfterEvent event emitted.
func (consumer *Consumer) Consume() error {
	topics := []string{}
	for topic := range consumer.Topics {
		topics = append(topics, topic)
	}

	cluster, err := cluster.NewConsumerFromClient(consumer.client, consumer.Group, topics)
	if err != nil {
		return err
	}

	consumer.cluster = cluster
	for message := range consumer.cluster.Messages() {
		consumer.EmitEvent(BeforeEvent, message)

		for _, subscription := range consumer.Topics[message.Topic] {
			subscription <- message
		}

		// The AfterEvent does not have to be called synchronously
		go consumer.EmitEvent(AfterEvent, message)
	}

	return nil
}

// Close closes all topic subscriptions and event channels.
func (consumer *Consumer) Close() {
	for topic, subscriptions := range consumer.Topics {
		for _, subscription := range subscriptions {
			consumer.UnSubscribe(topic, subscription)
		}
	}

	for event, subscriptions := range consumer.events {
		for _, subscription := range subscriptions {
			consumer.UnsubscribeEvent(event, subscription)
		}
	}
}

// EmitEvent calls all subscriptions of the given event.
// All subscriptions get called in a sync manner to allow consumer message
// to be manipulated.
func (consumer *Consumer) EmitEvent(event string, message *sarama.ConsumerMessage) {
	for _, subscription := range consumer.events[event] {
		subscription <- message
	}
}

// OnEvent creates a new event subscription for the given events.
// A channel and close function will be returned. The channel will receive consumer messages once the
// consumer emits the given event. The returned channel gets called in a sync
// manner to allowe consumer messages to be manipulated. When the close function gets
// called will the subscription be removed from the subscribed events list.
func (consumer *Consumer) OnEvent(event string) (<-chan *sarama.ConsumerMessage, func()) {
	subscription := make(chan *sarama.ConsumerMessage, 1)

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	consumer.events[event] = append(consumer.events[event], subscription)

	return subscription, func() {
		consumer.UnsubscribeEvent(event, subscription)
	}
}

// UnsubscribeEvent unsubscribes and closes the given channel from the given event.
// A boolean is returned that represents if the method found the channel or not.
func (consumer *Consumer) UnsubscribeEvent(event string, subscription chan *sarama.ConsumerMessage) bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for index, channel := range consumer.events[event] {
		if channel == subscription {
			close(subscription)
			consumer.events[event] = append(consumer.events[event][:index], consumer.events[event][index+1:]...)
			return true
		}
	}

	return false
}

// Subscribe creates a new topic subscription channel that will start to receive
// messages consumed by the consumer of the given topic. A channel and a closing method
// will be returned once the channel has been subscribed to the given topic.
func (consumer *Consumer) Subscribe(topic string) (<-chan *sarama.ConsumerMessage, func()) {
	subscription := make(chan *sarama.ConsumerMessage, 1)

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	consumer.Topics[topic] = append(consumer.Topics[topic], subscription)

	return subscription, func() {
		consumer.UnSubscribe(topic, subscription)
	}
}

// UnSubscribe unsubscribes the given channel from the given topic.
// A boolean is returned that represents if the method found the channel or not.
func (consumer *Consumer) UnSubscribe(topic string, channel chan *sarama.ConsumerMessage) bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for index, subscription := range consumer.Topics[topic] {
		if subscription == channel {
			close(subscription)
			consumer.Topics[topic] = append(consumer.Topics[topic][:index], consumer.Topics[topic][index+1:]...)
			return true
		}
	}

	return false
}
