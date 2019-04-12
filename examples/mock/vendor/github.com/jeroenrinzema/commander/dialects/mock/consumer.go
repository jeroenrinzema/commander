package mock

import (
	"sync"

	"github.com/jeroenrinzema/commander/types"
)

// Consumer a message consumer
type Consumer struct {
	subscriptions map[string]*SubscriptionCollection
	mutex         sync.RWMutex
	consumptions  sync.WaitGroup
}

// Emit emits a message to all subscribers of the given topic
// Once a message is passed to a subscription is
func (consumer *Consumer) Emit(message types.Message) {
	defer consumer.consumptions.Done()
	consumer.mutex.RLock()
	defer consumer.mutex.RUnlock()

	topic := message.Topic
	collection := consumer.subscriptions[topic.Name]

	if collection == nil {
		return
	}

	if len(collection.list) == 0 {
		return
	}

	collection.mutex.RLock()
	defer collection.mutex.RUnlock()

	wg := sync.WaitGroup{}
	wg.Add(len(collection.list))

	for _, subscription := range collection.list {
		go func(subscription *Subscription) {
			defer wg.Done()
			subscription.messages <- &message
			err := <-subscription.marked
			if err != nil {
				// TODO: handle error
			}
		}(subscription)
	}

	wg.Wait()
}

// Subscribe creates a new topic subscription that will receive
// messages consumed by the consumer of the given topic. This method
// will return a message channel and a close function.
// Once a message is consumed should the marked channel be called. Pass a nil for a successful consume and
// a error if a error occurred during processing.
func (consumer *Consumer) Subscribe(topics ...types.Topic) (<-chan *types.Message, chan<- error, error) {
	subscription := &Subscription{
		messages: make(chan *types.Message, 1),
		marked:   make(chan error, 1),
	}

	consumer.mutex.RLock()
	defer consumer.mutex.RUnlock()

	for _, topic := range topics {
		collection := consumer.subscriptions[topic.Name]

		if collection == nil {
			consumer.subscriptions[topic.Name] = &SubscriptionCollection{
				list: []*Subscription{},
			}

			collection = consumer.subscriptions[topic.Name]
		}

		collection.mutex.Lock()
		collection.list = append(collection.list, subscription)
		collection.mutex.Unlock()
	}

	return subscription.messages, subscription.marked, nil
}

// Unsubscribe unsubscribes the given channel subscription from the given topic.
// A boolean is returned that represents if the channel successfully got unsubscribed.
func (consumer *Consumer) Unsubscribe(channel <-chan *types.Message) error {
	consumer.mutex.RLock()
	defer consumer.mutex.RUnlock()

	for _, collection := range consumer.subscriptions {
		collection.mutex.RLock()

		for index, subscription := range collection.list {
			if subscription.messages == channel {
				collection.mutex.RUnlock()
				collection.mutex.Lock()
				collection.list = append(collection.list[:index], collection.list[index+1:]...)
				collection.mutex.Unlock()
				collection.mutex.RLock()
				break
			}
		}

		collection.mutex.RUnlock()
	}

	return nil
}

// Close closes the kafka consumer, all topic subscriptions and event channels.
func (consumer *Consumer) Close() error {
	consumer.consumptions.Wait()
	return nil
}
