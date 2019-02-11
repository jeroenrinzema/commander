package commander

import (
	"sync"
)

// MockDialect represents the mock dialect
type MockDialect struct {
	consumer *MockConsumer
	producer *MockProducer
}

// Open opens a mock consumer and producer
func (dialect *MockDialect) Open(connectionstring string, groups ...*Group) (Consumer, Producer, error) {
	consumer := &MockConsumer{
		subscriptions: make(map[string][]chan *Message),
	}

	producer := &MockProducer{
		consumer,
	}

	dialect.consumer = consumer
	dialect.producer = producer

	return consumer, producer, nil
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

// MockConsumer consumes kafka messages
type MockConsumer struct {
	subscriptions map[string][]chan *Message
	mutex         sync.Mutex
	consumptions  sync.WaitGroup
}

// Emit emits a message to all subscribers of the given topic
func (consumer *MockConsumer) Emit(message *Message) {
	consumer.mutex.Lock()
	consumer.consumptions.Add(1)

	topic := message.Topic.Name
	for _, subscription := range consumer.subscriptions[topic] {
		subscription <- message
	}

	consumer.consumptions.Done()
	consumer.mutex.Unlock()
}

// Subscribe subscribes to the given topics and returs a message channel
func (consumer *MockConsumer) Subscribe(topics ...Topic) (<-chan *Message, error) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	subscription := make(chan *Message, 1)
	for _, topic := range topics {
		if consumer.subscriptions[topic.Name] == nil {
			consumer.subscriptions[topic.Name] = []chan *Message{}
		}

		consumer.subscriptions[topic.Name] = append(consumer.subscriptions[topic.Name], subscription)
	}

	return subscription, nil
}

// Unsubscribe unsubscribes the given topic from the subscription list
func (consumer *MockConsumer) Unsubscribe(channel <-chan *Message) error {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for topic, subscriptions := range consumer.subscriptions {
		for index, subscription := range subscriptions {
			if subscription == channel {
				consumer.subscriptions[topic] = append(consumer.subscriptions[topic][:index], consumer.subscriptions[topic][index+1:]...)
			}
		}
	}

	return nil
}

// Close closes the kafka consumer
func (consumer *MockConsumer) Close() error {
	consumer.consumptions.Wait()
	return nil
}

// MockProducer produces kafka messages
type MockProducer struct {
	consumer *MockConsumer
}

// Publish publishes the given message
func (producer *MockProducer) Publish(message *Message) error {
	producer.consumer.Emit(message)
	return nil
}

// Close closes the kafka producer
func (producer *MockProducer) Close() error {
	return nil
}
