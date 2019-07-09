package mock

import (
	"sync"

	"github.com/jeroenrinzema/commander/types"
)

// Consumer a message consumer
type Consumer struct {
	subscriptions map[string]*SubscriptionCollection
	consumptions  sync.WaitGroup
	mutex         sync.RWMutex
}

// Emit emits a message to all subscribers of the given topic
// Once a message is passed to a subscription is
func (consumer *Consumer) Emit(message types.Message) {
	defer consumer.consumptions.Done()

	// FIXME: consumer mutex causes performance spikes during benchmarks
	consumer.mutex.RLock()
	defer consumer.mutex.RUnlock()

	topic := message.Topic
	collection := consumer.subscriptions[topic.Name]

	if collection == nil {
		return
	}

	collection.mutex.RLock()
	defer collection.mutex.RUnlock()

	if len(collection.list) == 0 {
		return
	}

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
func (consumer *Consumer) Subscribe(topics ...types.Topic) (<-chan *types.Message, func(error), error) {
	subscription := &Subscription{
		messages: make(chan *types.Message, 0),
		marked:   make(chan error, 0),
	}

	for _, topic := range topics {
		consumer.mutex.Lock()
		if consumer.subscriptions[topic.Name] == nil {
			consumer.subscriptions[topic.Name] = NewTopic()
		}

		consumer.subscriptions[topic.Name].list[subscription.messages] = subscription
		consumer.mutex.Unlock()
	}

	next := func(err error) {
		consumer.mutex.Lock()
		// Non blocking channel operation
		select {
		case subscription.marked <- err:
		default:
		}
		consumer.mutex.Unlock()
	}

	return subscription.messages, next, nil
}

// Unsubscribe unsubscribes the given channel subscription from the given topic.
// A boolean is returned that represents if the channel successfully got unsubscribed.
func (consumer *Consumer) Unsubscribe(sub <-chan *types.Message) error {
	consumer.mutex.RLock()
	defer consumer.mutex.RUnlock()

	for _, collection := range consumer.subscriptions {
		collection.mutex.Lock()
		subscription, has := collection.list[sub]
		if has {
			delete(collection.list, sub)
			close(subscription.messages)
			close(subscription.marked)
		}
		collection.mutex.Unlock()
	}
	return nil
}

// Close closes the kafka consumer, all topic subscriptions and event channels.
func (consumer *Consumer) Close() error {
	consumer.consumptions.Wait()
	return nil
}
