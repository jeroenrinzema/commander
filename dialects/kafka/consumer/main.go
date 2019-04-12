package consumer

import (
	"context"
	"errors"
	"sync"

	"github.com/Shopify/sarama"
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
func NewClient(brokers []string, group string, initialOffset int64, config *sarama.Config, ts ...types.Topic) (*Client, error) {
	topics := []string{}
	for _, topic := range ts {
		topics = append(topics, topic.Name)
	}

	client := &Client{
		brokers:  brokers,
		topics:   topics,
		channels: make(map[string]*Channel),
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

// Channel represents a thread safe list of subscriptions
type Channel struct {
	subscriptions []*Subscription
	mutex         sync.RWMutex
}

// Client consumes kafka messages
type Client struct {
	handle   Handle
	brokers  []string
	topics   []string
	channels map[string]*Channel
	mutex    sync.RWMutex
	ready    chan bool
}

// Subscribe subscribes to the given topics and returs a message channel
func (client *Client) Subscribe(topics ...types.Topic) (<-chan *types.Message, chan<- error, error) {
	subscription := &Subscription{
		marked:   make(chan error, 1),
		messages: make(chan *types.Message, 1),
	}

	client.mutex.Lock()
	defer client.mutex.Unlock()

	for _, topic := range topics {
		if client.channels[topic.Name] == nil {
			client.channels[topic.Name] = &Channel{}
		}

		client.channels[topic.Name].mutex.Lock()
		client.channels[topic.Name].subscriptions = append(client.channels[topic.Name].subscriptions, subscription)
		client.channels[topic.Name].mutex.Unlock()
	}

	return subscription.messages, subscription.marked, nil
}

// Unsubscribe unsubscribes the given topic from the subscription list
func (client *Client) Unsubscribe(sub <-chan *types.Message) error {
	client.mutex.RLock()
	defer client.mutex.RUnlock()

	for topic, channel := range client.channels {
		subscriptions := channel.subscriptions

		for index, subscription := range subscriptions {
			if subscription.messages == sub {
				channel.mutex.Lock()
				client.channels[topic].subscriptions = append(client.channels[topic].subscriptions[:index], client.channels[topic].subscriptions[index+1:]...)
				close(subscription.messages)
				channel.mutex.Unlock()
				break
			}
		}

	}

	return nil
}

// Claim consumes and emit's the given Kafka message to the subscribed
// subscriptions. All subscriptions are awaited untill done. An error
// is returned if one of the subscriptions failed to process the message.
func (client *Client) Claim(message *sarama.ConsumerMessage) error {
	var err error

	channel := client.channels[message.Topic]
	if channel != nil {
		subscriptions := channel.subscriptions
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

		channel.mutex.RLock()

		for _, subscription := range subscriptions {
			subscription.messages <- message
			err = <-subscription.marked
			if err != nil {
				break
			}
		}

		channel.mutex.RUnlock()
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
