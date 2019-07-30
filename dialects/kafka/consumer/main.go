package consumer

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander/dialects/kafka/metadata"
	"github.com/jeroenrinzema/commander/types"
)

// HandleType represents the type of consumer that is adviced to use for the given connectionstring
type HandleType int8

// Plausible consumer types
const (
	PartitionConsumerHandle HandleType = 0
	GroupConsumerHandle     HandleType = 1
)

// NewClient initializes a new consumer client and a Kafka consumer
func NewClient(brokers []string, group string) *Client {
	client := &Client{
		brokers: brokers,
		topics:  make(map[string]*Topic),
		group:   group,
	}

	return client
}

// Handle represents a Kafka consumer handle
type Handle interface {
	Close() error
}

// Claimer represents a consumer message claimer struct
type Claimer interface {
	Claim(*sarama.ConsumerMessage)
}

// Subscription represents a consumer topic(s) subscription
type Subscription struct {
	messages chan *types.Message
	marked   chan error
}

// Topic represents a thread safe list of subscriptions
type Topic struct {
	subscriptions map[<-chan *types.Message]*Subscription
	mutex         sync.RWMutex
}

// NewTopic constructs and returns a new Topic struct
func NewTopic() *Topic {
	return &Topic{
		subscriptions: make(map[<-chan *types.Message]*Subscription),
	}
}

// Client consumes kafka messages
type Client struct {
	handle  Handle
	brokers []string
	topics  map[string]*Topic
	ready   chan bool
	group   string
}

// Connect opens a new Kafka consumer
func (client *Client) Connect(initialOffset int64, config *sarama.Config, ts ...types.Topic) error {
	topics := []string{}
	for _, topic := range ts {
		topics = append(topics, topic.Name)
	}

	if client.group != "" {
		handle := NewGroupHandle(client)
		err := handle.Connect(client.brokers, topics, client.group, config)
		if err != nil {
			return err
		}

		client.handle = handle
		return nil
	}

	handle := NewPartitionHandle(client)
	err := handle.Connect(client.brokers, topics, initialOffset, config)
	if err != nil {
		return err
	}

	client.handle = handle
	return nil
}

// Subscribe subscribes to the given topics and returs a message channel
func (client *Client) Subscribe(topics ...types.Topic) (<-chan *types.Message, func(error), error) {
	subscription := &Subscription{
		marked:   make(chan error, 1),
		messages: make(chan *types.Message, 1),
	}

	for _, topic := range topics {
		if client.topics[topic.Name] == nil {
			client.topics[topic.Name] = NewTopic()
		}

		client.topics[topic.Name].subscriptions[subscription.messages] = subscription
	}

	next := func(err error) {
		subscription.marked <- err
	}

	return subscription.messages, next, nil
}

// Unsubscribe removes the given channel from the available subscriptions.
// A new goroutine is spawned to avoid locking the channel.
func (client *Client) Unsubscribe(sub <-chan *types.Message) error {
	for _, topic := range client.topics {
		go func(topic *Topic, sub <-chan *types.Message) {
			topic.mutex.Lock()
			subscription, has := topic.subscriptions[sub]
			if has {
				delete(topic.subscriptions, sub)
				close(subscription.messages)
				close(subscription.marked)
			}
			topic.mutex.Unlock()
		}(topic, sub)
	}

	return nil
}

// Claim consumes and emit's the given Kafka message to the subscribed
// subscriptions. All subscriptions are awaited untill done. An error
// is returned if one of the subscriptions failed to process the message.
func (client *Client) Claim(consumed *sarama.ConsumerMessage) (err error) {
	topic := consumed.Topic

	if client.topics[topic] == nil {
		return nil
	}

	message := metadata.MessageFromMessage(consumed)

	client.topics[topic].mutex.RLock()
	for _, subscription := range client.topics[topic].subscriptions {
		select {
		case subscription.messages <- message:
			err = <-subscription.marked
			if err != nil {
				break
			}
		default:
		}
	}
	client.topics[topic].mutex.RUnlock()

	return err
}

// Close closes the Kafka consumer
func (client *Client) Close() error {
	if client.handle == nil {
		return nil
	}

	return client.handle.Close()
}
