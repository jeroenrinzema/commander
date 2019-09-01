package consumer

import (
	"errors"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander/dialects/kafka/metadata"
	"github.com/jeroenrinzema/commander/types"
)

var (
	// ErrRetry error retry type representation
	ErrRetry = errors.New("retry message")
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
	conn    sarama.Client
	group   string
}

// Healthy checks the health of the Kafka client
func (client *Client) Healthy() bool {
	if len(client.conn.Brokers()) == 0 {
		return false
	}

	return true
}

// Connect opens a new Kafka consumer
func (client *Client) Connect(brokers []string, config *sarama.Config, initialOffset int64, ts ...types.Topic) error {
	conn, err := sarama.NewClient(brokers, config)
	if err != nil {
		return err
	}

	topics := []string{}
	for _, topic := range ts {
		topics = append(topics, topic.Name())
	}

	client.conn = conn

	if client.group != "" {
		handle := NewGroupHandle(client)
		err := handle.Connect(conn, topics, client.group)
		if err != nil {
			return err
		}

		client.handle = handle
		return nil
	}

	handle := NewPartitionHandle(client)
	err = handle.Connect(conn, topics, initialOffset)
	if err != nil {
		return err
	}

	client.handle = handle
	return nil
}

// Subscribe subscribes to the given topics and returs a message channel
func (client *Client) Subscribe(topics ...types.Topic) (<-chan *types.Message, error) {
	subscription := &Subscription{
		messages: make(chan *types.Message, 0),
	}

	for _, topic := range topics {
		if client.topics[topic.Name()] == nil {
			client.topics[topic.Name()] = NewTopic()
		}

		client.topics[topic.Name()].subscriptions[subscription.messages] = subscription
	}

	return subscription.messages, nil
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
	defer client.topics[topic].mutex.RUnlock()

	for _, subscription := range client.topics[topic].subscriptions {
		message.Async()
		select {
		case subscription.messages <- message:
			result := message.Await()
			if result != nil {
				return ErrRetry
			}
		default:
		}
	}

	return nil
}

// Close closes the Kafka consumer
func (client *Client) Close() error {
	if client.handle == nil {
		return nil
	}

	return client.handle.Close()
}
