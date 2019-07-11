package mock

import (
	"sync"

	"github.com/jeroenrinzema/commander/types"
)

// Consumer a message consumer
type Consumer struct {
	subscriptions map[string]*SubscriptionCollection
	workers       int8
	queue         chan *types.Message
	consumptions  sync.WaitGroup
	mutex         sync.RWMutex
}

// NewWorker spawns a new queue worker
func (consumer *Consumer) NewWorker() {
	for message := range consumer.queue {
		if message == nil {
			break
		}

		consumer.mutex.RLock()
		consumer.consumptions.Add(1)

		collection, has := consumer.subscriptions[message.Topic.Name]
		if !has {
			continue
		}

		for _, subscription := range collection.list {
			subscription.messages <- message
			err := <-subscription.marked
			if err != nil {
				// TODO: handle error
			}
		}

		consumer.consumptions.Done()
		consumer.mutex.RUnlock()
	}
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

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for _, topic := range topics {
		if consumer.subscriptions[topic.Name] == nil {
			consumer.subscriptions[topic.Name] = NewTopic()
		}

		consumer.subscriptions[topic.Name].list[subscription.messages] = subscription
	}

	// FIXME: currently the working limit is set to one
	if consumer.workers == 0 {
		consumer.workers++
		go consumer.NewWorker()
	}

	// marked could safely be written to due to the expected
	// mutex locks while emitting a message.
	next := func(err error) {
		subscription.marked <- err
	}

	return subscription.messages, next, nil
}

// Unsubscribe unsubscribes the given channel subscription from the given topic.
// A boolean is returned that represents if the channel successfully got unsubscribed.
func (consumer *Consumer) Unsubscribe(sub <-chan *types.Message) error {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for _, collection := range consumer.subscriptions {
		subscription, has := collection.list[sub]
		if has {
			delete(collection.list, sub)
			close(subscription.messages)
			close(subscription.marked)
		}
	}

	return nil
}

// Close closes the kafka consumer, all topic subscriptions and event channels.
func (consumer *Consumer) Close() error {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	consumer.consumptions.Wait()
	close(consumer.queue)

	for key := range consumer.subscriptions {
		delete(consumer.subscriptions, key)
	}
	return nil
}
