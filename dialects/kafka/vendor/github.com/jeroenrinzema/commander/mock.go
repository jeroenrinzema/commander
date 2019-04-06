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

// MockDialect a in-memory mocking dialect
type MockDialect struct {
	Consumer *MockConsumer
	Producer *MockProducer
}

// Open opens the mocking consumer and producer
func (dialect *MockDialect) Open(connectionstring string, groups ...*Group) (Consumer, Producer, error) {
	consumer := &MockConsumer{
		subscriptions: make(map[string][]*MockSubscription),
	}

	producer := &MockProducer{
		consumer,
	}

	dialect.Consumer = consumer
	dialect.Producer = producer

	return consumer, producer, nil
}

// Healthy checks if the dialect is healthy and up and running
func (dialect *MockDialect) Healthy() bool {
	return true
}

// Close closes the mock dialect
func (dialect *MockDialect) Close() error {
	dialect.Consumer.Close()
	dialect.Producer.Close()

	return nil
}

// MockConsumer consumes messages and emits them to the subscribed channels
type MockConsumer struct {
	subscriptions map[string][]*MockSubscription
	mutex         sync.RWMutex
	consumptions  sync.WaitGroup
}

// Emit emits a message to all subscribers of the given topic
func (consumer *MockConsumer) Emit(message *Message) {
	Logger.Println("claimed message from:", message.Topic.Name)

	consumer.consumptions.Add(1)
	defer consumer.consumptions.Done()

	consumer.mutex.RLock()
	defer consumer.mutex.RUnlock()

	message.Timestamp = time.Now()
	topic := message.Topic.Name
	for _, subscription := range consumer.subscriptions[topic] {
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

	consumer.mutex.Lock()

	for _, topic := range topics {
		if consumer.subscriptions[topic.Name] == nil {
			consumer.subscriptions[topic.Name] = []*MockSubscription{}
		}

		consumer.subscriptions[topic.Name] = append(consumer.subscriptions[topic.Name], subscription)
	}

	consumer.mutex.Unlock()

	return subscription.messages, subscription.marked, nil
}

// Unsubscribe unsubscribes the given consumer channel (if found) from the subscription list
func (consumer *MockConsumer) Unsubscribe(channel <-chan *Message) error {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for topic, subscriptions := range consumer.subscriptions {
		for index, subscription := range subscriptions {
			if subscription.messages == channel {
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
