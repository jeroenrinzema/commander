package mock

import (
	"sync"

	"github.com/jeroenrinzema/commander"
)

// Consumer consumes kafka messages
type Consumer struct {
	subscriptions map[string][]chan *commander.Message
	mutex         sync.Mutex
	consumptions  sync.WaitGroup
}

// Emit emits a message to all subscribers of the given topic
func (consumer *Consumer) Emit(message *commander.Message) {
	consumer.mutex.Lock()
	consumer.consumptions.Add(1)

	topic := message.Topic.Name
	if consumer.subscriptions[topic] != nil {
		for _, subscription := range consumer.subscriptions[topic] {
			subscription <- message
		}
	}

	consumer.consumptions.Done()
	consumer.mutex.Unlock()
}

// Subscribe subscribes to the given topics and returs a message channel
func (consumer *Consumer) Subscribe(topics ...commander.Topic) (<-chan *commander.Message, error) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	subscription := make(chan *commander.Message, 1)
	for _, topic := range topics {
		subscriptions := consumer.subscriptions[topic.Name]

		if subscriptions == nil {
			subscriptions = []chan *commander.Message{}
		}

		subscriptions = append(subscriptions, subscription)
		consumer.subscriptions[topic.Name] = subscriptions
	}

	return subscription, nil
}

// Unsubscribe unsubscribes the given topic from the subscription list
func (consumer *Consumer) Unsubscribe(channel <-chan *commander.Message) error {
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
func (consumer *Consumer) Close() error {
	consumer.consumptions.Wait()
	return nil
}
