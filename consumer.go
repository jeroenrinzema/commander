package commander

import (
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// TopicType represents the various topic types
type TopicType int8

// Represents the various topic types
const (
	EventTopic   TopicType = 1
	CommandTopic TopicType = 2
)

// Topic contains information of a kafka topic
type Topic struct {
	Name    string
	Type    TopicType
	Produce bool
	Consume bool
}

// KafkaConsumer is the interface used to consume kafka messages
type KafkaConsumer interface {
	SubscribeTopics([]string, kafka.RebalanceCb) error
	Events() chan kafka.Event

	Assign([]kafka.TopicPartition) error
	Unassign() error
	Close() error
}

// ConsumerEventHandle hadnles events emitted by the consumer
type ConsumerEventHandle func(kafka.Event)

// NewConsumer initalizes a new kafka consumer and consumer instance.
func NewConsumer(config *kafka.ConfigMap) (Consumer, error) {
	config.SetKey("go.events.channel.enable", true)
	config.SetKey("go.application.rebalance.enable", true)

	client, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	consumer := &consumer{
		config:  config,
		topics:  make(map[string][]chan *kafka.Message),
		events:  make(map[string][]ConsumerEventHandle),
		client:  client,
		closing: make(chan bool, 1),
	}

	return consumer, nil
}

// Consumer this consumer consumes messages from a
// kafka topic. A channel is opened to receive kafka messages
type Consumer interface {
	// AddGroups collects and subscribes to all topics that do not have the IgnoreConsumption property set to true.
	AddGroups(...*Group) error

	// SetTopics initializes a channel for the given topics that do not exist.
	// This method will ignore all topics that have their IgnoreConsumption property set to true.
	SetTopics(topics ...Topic)

	// SubscribeTopics subscribed to all registered topics.
	SubscribeTopics() error

	// UseMockConsumer replaces the current consumer with a mock consumer.
	// This method is mainly used for testing purposes
	UseMockConsumer() *MockKafkaConsumer

	// Consume opens the kafka consumer and emits consumed messages to the subscribed subscriptions.
	// A topic subscription could be made with the Subscribe method.
	// Before a message is passed on to any topic subscription is a BeforeEvent event with the received message emitted.
	// After the message has been consumed and processed is the AfterEvent event emitted.
	Consume()

	// BeforeClosing returns a channel that gets called before the consumer gets closed
	BeforeClosing() <-chan bool

	// Close closes the kafka consumer, all topic subscriptions and event channels.
	Close()

	// EmitEvent calls all subscriptions for the given event.
	// All subscriptions get called in a sync manner to allow the consumed message to be manipulated.
	EmitEvent(string, kafka.Event)

	// OnEvent creates a new event subscription for the given event.
	// The method will return a close function to unsubscribe the handle method.
	// Once a event messaged is emitted will it be passed to given ConsumerEventHandle.
	OnEvent(string, ConsumerEventHandle) func()

	// OffEvent unsubscribes the given handle from the given event.
	// A boolean is returned that represents if the handle method was found or not.
	OffEvent(string, ConsumerEventHandle) bool

	// Subscribe creates a new topic subscription that will receive
	// messages consumed by the consumer of the given topic. This method
	// will return a message channel and a close function.
	Subscribe(...Topic) (<-chan *kafka.Message, Closing)

	// Unsubscribe unsubscribes the given channel subscription from the given topic.
	// A boolean is returned that represents if the channel successfully got unsubscribed.
	Unsubscribe(<-chan *kafka.Message) bool
}

type consumer struct {
	topics map[string][]chan *kafka.Message
	group  string

	config *kafka.ConfigMap
	client KafkaConsumer

	closing      chan bool
	mutex        sync.Mutex
	consumptions sync.WaitGroup
	events       map[string][]ConsumerEventHandle
}

func (consumer *consumer) UseMockConsumer() *MockKafkaConsumer {
	mock := &MockKafkaConsumer{
		events: make(chan kafka.Event),
	}

	consumer.client = mock
	return mock
}

func (consumer *consumer) AddGroups(groups ...*Group) error {
	consumer.mutex.Lock()

	for _, group := range groups {
		topics := []Topic{}

		for _, topic := range group.Topics {
			if topic.Consume != true {
				continue
			}

			topics = append(topics, topic)
		}

		consumer.SetTopics(topics...)
	}

	consumer.mutex.Unlock()

	err := consumer.SubscribeTopics()
	if err != nil {
		return err
	}

	return nil
}

func (consumer *consumer) SetTopics(topics ...Topic) {
	for _, topic := range topics {
		if topic.Consume == false {
			continue
		}

		_, has := consumer.topics[topic.Name]

		if has {
			continue
		}

		consumer.topics[topic.Name] = []chan *kafka.Message{}
	}
}

func (consumer *consumer) SubscribeTopics() error {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	topics := []string{}
	for topic := range consumer.topics {
		topics = append(topics, topic)
	}

	err := consumer.client.SubscribeTopics(topics, nil)
	return err
}

func (consumer *consumer) Consume() {
	for {
		select {
		case event := <-consumer.client.Events():
			if event == nil {
				continue
			}

			consumer.consumptions.Add(1)
			consumer.EmitEvent(BeforeEvent, event)

			switch message := event.(type) {
			case kafka.AssignedPartitions:
				consumer.client.Assign(message.Partitions)
			case kafka.RevokedPartitions:
				consumer.client.Unassign()
			case *kafka.Message:
				consumer.mutex.Lock()
				for _, subscription := range consumer.topics[*message.TopicPartition.Topic] {
					subscription <- message
				}
				consumer.mutex.Unlock()
			}

			consumer.EmitEvent(AfterEvent, event)
			consumer.consumptions.Done()
		}
	}
}

func (consumer *consumer) BeforeClosing() <-chan bool {
	return consumer.closing
}

func (consumer *consumer) Close() {
	close(consumer.closing)

	consumer.client.Close()
	consumer.consumptions.Wait()

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for topic, subscriptions := range consumer.topics {
		for _, subscription := range subscriptions {
			close(subscription)
		}

		consumer.topics[topic] = nil
	}

	for event := range consumer.events {
		consumer.events[event] = nil
	}
}

func (consumer *consumer) EmitEvent(name string, event kafka.Event) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for _, subscription := range consumer.events[name] {
		subscription(event)
	}
}

func (consumer *consumer) OnEvent(event string, handle ConsumerEventHandle) func() {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	consumer.events[event] = append(consumer.events[event], handle)

	return func() {
		consumer.OffEvent(event, handle)
	}
}

func (consumer *consumer) OffEvent(event string, handle ConsumerEventHandle) bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for index, subscription := range consumer.events[event] {
		if &handle == &subscription {
			consumer.events[event] = append(consumer.events[event][:index], consumer.events[event][index+1:]...)
			return true
		}
	}

	return false
}

func (consumer *consumer) Subscribe(topics ...Topic) (<-chan *kafka.Message, Closing) {
	for _, topic := range topics {
		if topic.Consume == false {
			panic(fmt.Sprintf("The topic %s is ignored for consumption", topic.Name))
		}
	}

	subscription := make(chan *kafka.Message, 1)

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for _, topic := range topics {
		consumer.topics[topic.Name] = append(consumer.topics[topic.Name], subscription)
	}

	return subscription, func() {
		consumer.Unsubscribe(subscription)
	}
}

func (consumer *consumer) Unsubscribe(channel <-chan *kafka.Message) bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for topic := range consumer.topics {
		for index, subscription := range consumer.topics[topic] {
			if subscription == channel {
				close(subscription)
				consumer.topics[topic] = append(consumer.topics[topic][:index], consumer.topics[topic][index+1:]...)
				return true
			}
		}
	}

	return false
}
