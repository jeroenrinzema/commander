package commander

import (
	"sync"
	"time"
)

// MockSubscription mock message subscription
type MockSubscription struct {
	messages chan *Message
	marked   chan error
}

// MockTopicSubscriptions holds the active mock subscriptions for the given topic
type MockTopicSubscriptions struct {
	list  []*MockSubscription
	mutex sync.RWMutex
}

// NewMockDialect opens and constructs a in-memory mocking dialect
func NewMockDialect() *MockDialect {
	consumer := &MockConsumer{
		subscriptions: make(map[string]*MockTopicSubscriptions),
	}

	producer := &MockProducer{
		consumer,
	}

	dialect := &MockDialect{
		consumer: consumer,
		producer: producer,
	}

	return dialect
}

// MockDialect a in-memory mocking dialect
type MockDialect struct {
	consumer *MockConsumer
	producer *MockProducer
}

// Consumer returns the dialect consumer
func (dialect *MockDialect) Consumer() Consumer {
	return dialect.consumer
}

// Producer returns the dialect producer
func (dialect *MockDialect) Producer() Producer {
	return dialect.producer
}

// Healthy checks if the dialect is healthy and up and running
func (dialect *MockDialect) Healthy() bool {
	return true
}

// Close closes the mock dialect
func (dialect *MockDialect) Close() error {
	dialect.consumer.Close()
	dialect.producer.Close()

	return nil
}

// MockConsumer consumes messages and emits them to the subscribed channels
type MockConsumer struct {
	subscriptions map[string]*MockTopicSubscriptions
	mutex         sync.RWMutex
	consumptions  sync.WaitGroup
}

// Emit emits a message to all subscribers of the given topic
func (consumer *MockConsumer) Emit(message *Message) {
	Logger.Println("claimed message from:", message.Topic.Name)

	consumer.mutex.RLock()
	defer consumer.mutex.RUnlock()

	consumer.consumptions.Add(1)
	defer consumer.consumptions.Done()

	message.Timestamp = time.Now()
	topic := message.Topic.Name

	if consumer.subscriptions[topic] == nil {
		return
	}

	consumer.subscriptions[topic].mutex.RLock()
	defer consumer.subscriptions[topic].mutex.RUnlock()

	for _, subscription := range consumer.subscriptions[topic].list {
		subscription.messages <- message
		err := <-subscription.marked
		if err != nil {
			// NOTE: should a panic really be thrown here?
			// An error has to be returned to the client (usually testing) but a panic is not expected
			panic(err)
		}
	}

	Logger.Println("message marked")
}

// Subscribe subscribes to the given topics and returs a message channel
func (consumer *MockConsumer) Subscribe(topics ...Topic) (<-chan *Message, chan<- error, error) {
	subscription := &MockSubscription{
		messages: make(chan *Message, 1),
		marked:   make(chan error, 1),
	}

	for _, topic := range topics {
		if consumer.subscriptions[topic.Name] == nil {
			consumer.mutex.Lock()
			consumer.subscriptions[topic.Name] = &MockTopicSubscriptions{
				list: []*MockSubscription{},
			}
			consumer.mutex.Unlock()
		}

		consumer.subscriptions[topic.Name].mutex.Lock()
		consumer.subscriptions[topic.Name].list = append(consumer.subscriptions[topic.Name].list, subscription)
		consumer.subscriptions[topic.Name].mutex.Unlock()
	}

	return subscription.messages, subscription.marked, nil
}

// Unsubscribe unsubscribes the given consumer channel (if found) from the subscription list
func (consumer *MockConsumer) Unsubscribe(channel <-chan *Message) error {
	consumer.mutex.RLock()
	defer consumer.mutex.RUnlock()

	for topic, subscriptions := range consumer.subscriptions {
		for index, subscription := range subscriptions.list {
			if subscription.messages == channel {
				consumer.subscriptions[topic].mutex.Lock()
				consumer.subscriptions[topic].list = append(consumer.subscriptions[topic].list[:index], consumer.subscriptions[topic].list[index+1:]...)
				consumer.subscriptions[topic].mutex.Unlock()
			}
		}
	}

	return nil
}

// Close closes the kafka consumer
func (consumer *MockConsumer) Close() error {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	consumer.consumptions.Wait()
	return nil
}

// MockProducer emits messages to the attached consumer
type MockProducer struct {
	consumer *MockConsumer
}

// Publish publishes the given message
func (producer *MockProducer) Publish(message *Message) error {
	go producer.consumer.Emit(message)
	return nil
}

// Close closes the kafka producer
func (producer *MockProducer) Close() error {
	return nil
}
