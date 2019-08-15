package mock

import (
	"sync"

	"github.com/jeroenrinzema/commander/types"
	log "github.com/sirupsen/logrus"
)

// Consumer a message consumer
type Consumer struct {
	subscriptions map[string]*SubscriptionCollection
	workers       int8
	consumptions  sync.WaitGroup
	mutex         sync.RWMutex
	logger        *log.Logger
}

// Emit emits the given message to the subscribed consumers
func (consumer *Consumer) Emit(message *types.Message) {
	consumer.logger.Debug("emitting message!")

	consumer.consumptions.Add(1)
	defer consumer.consumptions.Done()

	consumer.mutex.RLock()
	collection, has := consumer.subscriptions[message.Topic.Name]
	if !has {
		consumer.mutex.RUnlock()
		return
	}

	length := len(collection.list)
	if length == 0 {
		consumer.mutex.RUnlock()
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(len(collection.list))

	go func(collection *SubscriptionCollection, message *types.Message) {
		collection.mutex.Lock()
		for _, subscription := range collection.list {
			message.Async()
			subscription.messages <- message
			message.Await()
			wg.Done()
		}
		collection.mutex.Unlock()
	}(collection, message)

	consumer.mutex.RUnlock()
	wg.Wait()
}

// Subscribe creates a new topic subscription that will receive
// messages consumed by the consumer of the given topic. This method
// will return a message channel and a close function.
// Once a message is consumed should the marked channel be called. Pass a nil for a successful consume and
// a error if a error occurred during processing.
func (consumer *Consumer) Subscribe(topics ...types.Topic) (<-chan *types.Message, error) {
	subscription := &Subscription{
		messages: make(chan *types.Message, 0),
	}

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for _, topic := range topics {
		if consumer.subscriptions[topic.Name] == nil {
			consumer.subscriptions[topic.Name] = NewTopic()
		}

		consumer.subscriptions[topic.Name].list[subscription.messages] = subscription
	}

	consumer.logger.Debugf("subscribing to: %+v, %v", topics, subscription.messages)
	return subscription.messages, nil
}

// Unsubscribe unsubscribes the given channel subscription from the given topic.
// A boolean is returned that represents if the channel successfully got unsubscribed.
func (consumer *Consumer) Unsubscribe(sub <-chan *types.Message) error {
	consumer.logger.Debugf("unsubscribe: %v", sub)

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for _, collection := range consumer.subscriptions {
		subscription, has := collection.list[sub]
		if has {
			collection.mutex.Lock()
			delete(collection.list, sub)
			close(subscription.messages)
			collection.mutex.Unlock()
		}
	}

	return nil
}

// Close closes the kafka consumer, all topic subscriptions and event channels.
func (consumer *Consumer) Close() error {
	consumer.logger.Info("closing mock dialect consumer")

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	consumer.consumptions.Wait()

	for topic := range consumer.subscriptions {
		delete(consumer.subscriptions, topic)
	}
	return nil
}
