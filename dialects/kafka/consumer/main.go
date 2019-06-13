package consumer

import (
	"context"
	"errors"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander/types"
)

type nothing struct{}

// HandleType represents the type of consumer that is adviced to use for the given connectionstring
type HandleType int8

// Plausible consumer types
const (
	PartitionConsumerHandle HandleType = 0
	GroupConsumerHandle     HandleType = 1
)

// NewClient initializes a new consumer client and a Kafka consumer
func NewClient(brokers []string, group string, initialOffset int64, config *sarama.Config, ts ...types.Topic) (*Client, error) {
	topics := []string{}
	for _, topic := range ts {
		topics = append(topics, topic.Name)
	}

	client := &Client{
		brokers: brokers,
		topics:  make(map[string]*Topic),
	}

	if len(topics) == 0 {
		return client, nil
	}

	handle := MatchHandleType(brokers, group, config)
	switch handle {
	case GroupConsumerHandle:
		handle := NewGroupHandle(client)
		err := handle.Connect(brokers, topics, group, config)
		if err != nil {
			return nil, err
		}

		client.handle = handle
	case PartitionConsumerHandle:
		handle := NewPartitionHandle(client)
		err := handle.Connect(brokers, topics, initialOffset, config)
		if err != nil {
			return nil, err
		}

		client.handle = handle
	}

	if client.handle == nil {
		return nil, errors.New("No consumer handle has been set up")
	}

	return client, nil
}

// MatchHandleType creates an advice of which handle type should be used for consumption
func MatchHandleType(brokers []string, group string, config *sarama.Config) HandleType {
	if group != "" {
		return GroupConsumerHandle
	}

	return PartitionConsumerHandle
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
}

// Subscribe subscribes to the given topics and returs a message channel
func (client *Client) Subscribe(topics ...types.Topic) (<-chan *types.Message, chan<- error, error) {
	subscription := &Subscription{
		marked:   make(chan error, 1),
		messages: make(chan *types.Message, 1),
	}

	for _, topic := range topics {
		if client.topics[topic.Name] == nil {
			client.topics[topic.Name] = NewTopic()
		}

		client.topics[topic.Name].mutex.Lock()
		client.topics[topic.Name].subscriptions[subscription.messages] = subscription
		client.topics[topic.Name].mutex.Unlock()
	}

	return subscription.messages, subscription.marked, nil
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
func (client *Client) Claim(message *sarama.ConsumerMessage) error {
	var err error
	topic := message.Topic

	if client.topics[topic] != nil {
		headers := map[string]string{}
		for _, record := range message.Headers {
			headers[string(record.Key)] = string(record.Value)
		}

		message := &types.Message{
			Headers: headers,
			Topic: types.Topic{
				Name: message.Topic,
			},
			Offset:    int(message.Offset),
			Partition: int(message.Partition),
			Value:     message.Value,
			Key:       message.Key,
			Timestamp: message.Timestamp,
			Ctx:       context.Background(),
		}

		wg := sync.WaitGroup{}
		client.topics[topic].mutex.RLock()
		wg.Add(len(client.topics[topic].subscriptions))

		for _, subscription := range client.topics[topic].subscriptions {
			go func(subscription *Subscription) {
				defer wg.Done()
				select {
				case subscription.messages <- message:
					err = <-subscription.marked
					if err != nil {
						return
					}
				default:
				}
			}(subscription)
		}

		wg.Wait()
		client.topics[topic].mutex.RUnlock()
	}

	return err
}

// Close closes the Kafka consumer
func (client *Client) Close() error {
	if client.handle == nil {
		return nil
	}

	return client.handle.Close()
}
