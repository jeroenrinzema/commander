package commander

import (
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaConsumer is a consumer interface
type KafkaConsumer interface {
	SubscribeTopics([]string, kafka.RebalanceCb) error
	Events() chan kafka.Event

	Assign([]kafka.TopicPartition) error
	Unassign() error
	Close() error
}

// NewConsumer initalizes a new consumer struct with the given cluster client.
func NewConsumer(config *kafka.ConfigMap) (*Consumer, error) {
	config.SetKey("go.events.channel.enable", true)
	config.SetKey("go.application.rebalance.enable", true)

	cluster, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		config:  config,
		Topics:  make(map[string][]chan *kafka.Message),
		events:  make(map[string][]chan kafka.Event),
		kafka:   cluster,
		closing: make(chan bool),
	}

	return consumer, nil
}

// Consumer this consumer consumes messages from a
// kafka topic. A channel is opened to receive kafka messages
type Consumer struct {
	Topics map[string][]chan *kafka.Message
	Group  string

	config *kafka.ConfigMap
	kafka  KafkaConsumer

	closing chan bool
	mutex   sync.Mutex
	events  map[string][]chan kafka.Event
}

// NewGroups collects all topics that do not have the IgnoreConsumption
// property set to true.
func (consumer *Consumer) NewGroups(groups ...*Group) error {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for _, group := range groups {
		for _, t := range group.Topics {
			topic, ok := t.(MockTopic)
			if ok != true {
				continue
			}

			if topic.IgnoreConsumption {
				continue
			}

			_, has := consumer.Topics[topic.Name]

			if has {
				continue
			}

			consumer.Topics[topic.Name] = []chan kafka.Event{}
		}
	}

	err := consumer.SubscribeTopics()
	if err != nil {
		return err
	}

	return nil
}

// SubscribeTopics subscribed to all set topics.
func (consumer *Consumer) SubscribeTopics() error {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	topics := []string{}
	for topic := range consumer.Topics {
		topics = append(topics, topic)
	}

	err := consumer.kafka.SubscribeTopics(topics, nil)
	return err
}

// Consume subscribes to all given topics and creates a new consumer.
// The consumed messages get send to subscribed topic subscriptions.
// A topic subscription could be made with the consumer.Subscribe method.
// Before a message is passed on to any topic subscription is the BeforeEvent event emitted.
// And once a event has been consumed and processed is the AfterEvent event emitted.
func (consumer *Consumer) Consume() {
	for {
		select {
		case <-consumer.closing:
			break
		case event := <-consumer.kafka.Events():
			consumer.EmitEvent(BeforeEvent, event)

			switch message := event.(type) {
			case kafka.AssignedPartitions:
				consumer.kafka.Assign(message.Partitions)
			case kafka.RevokedPartitions:
				consumer.kafka.Unassign()
			case *kafka.Message:
				consumer.mutex.Lock()
				for _, subscription := range consumer.Topics[*message.TopicPartition.Topic] {
					subscription <- message
				}
				consumer.mutex.Unlock()
			}

			// The AfterEvent does not have to be called synchronously
			go consumer.EmitEvent(AfterEvent, event)
		}
	}
}

// BeforeClosing returns a channel that gets called before closing
func (consumer *Consumer) BeforeClosing() <-chan bool {
	return consumer.closing
}

// Close closes all topic subscriptions and event channels.
func (consumer *Consumer) Close() {
	close(consumer.closing)

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for topic, subscriptions := range consumer.Topics {
		for _, subscription := range subscriptions {
			close(subscription)
		}

		consumer.Topics[topic] = nil
	}

	for event, subscriptions := range consumer.events {
		for _, subscription := range subscriptions {
			close(subscription)
		}

		consumer.events[event] = nil
	}

	consumer.kafka.Close()
}

// EmitEvent calls all subscriptions of the given event.
// All subscriptions get called in a sync manner to allow consumer message
// to be manipulated.
func (consumer *Consumer) EmitEvent(name string, event kafka.Event) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for _, subscription := range consumer.events[name] {
		subscription <- event
	}
}

// OnEvent creates a new event subscription for the given events.
// A channel and close function will be returned. The channel will receive consumer messages once the
// consumer emits the given event. The returned channel gets called in a sync
// manner to allowe consumer messages to be manipulated. When the close function gets
// called will the subscription be removed from the subscribed events list.
func (consumer *Consumer) OnEvent(event string) (<-chan kafka.Event, func()) {
	subscription := make(chan kafka.Event, 1)

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	consumer.events[event] = append(consumer.events[event], subscription)

	return subscription, func() {
		consumer.OffEvent(event, subscription)
	}
}

// OffEvent unsubscribes and closes the given channel from the given event.
// A boolean is returned that represents if the method found the channel or not.
func (consumer *Consumer) OffEvent(event string, channel <-chan kafka.Event) bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for index, subscription := range consumer.events[event] {
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
func (consumer *Consumer) Subscribe(topic string) (<-chan *kafka.Message, func()) {
	subscription := make(chan *kafka.Message, 1)

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	consumer.Topics[topic] = append(consumer.Topics[topic], subscription)

	return subscription, func() {
		consumer.Unsubscribe(topic, subscription)
	}
}

// Unsubscribe unsubscribes the given channel from the given topic.
// A boolean is returned that represents if the method found the channel or not.
func (consumer *Consumer) Unsubscribe(topic string, channel <-chan *kafka.Message) bool {
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
