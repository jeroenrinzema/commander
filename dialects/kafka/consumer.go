package kafka

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander"
)

// NewConsumer constructs a new kafka dialect consumer
func NewConsumer(connectionstring Config, config *sarama.Config, groups ...*commander.Group) (*Consumer, error) {
	topics := []string{}

	for _, group := range groups {
		for _, topic := range group.Topics {
			if !topic.Consume {
				continue
			}

			topics = append(topics, topic.Name)
		}
	}

	consumer := &Consumer{
		topics:        topics,
		subscriptions: make(map[string][]chan *commander.Message),
		ready:         make(chan bool, 0),
	}

	commander.Logger.Println("Awaiting consumer setup")

	err := consumer.Connect(connectionstring, config)
	if err != nil {
		commander.Logger.Println(err)
		return nil, err
	}

	return consumer, nil
}

// Consumer consumes kafka messages
type Consumer struct {
	client           sarama.ConsumerGroup
	connectionstring Config
	config           *sarama.Config
	topics           []string
	subscriptions    map[string][]chan *commander.Message
	consumptions     sync.WaitGroup
	mutex            sync.RWMutex
	ready            chan bool
}

// Connect initializes a new Sarama consumer group and awaits till the consumer
// group is set up and ready to consume messages.
func (consumer *Consumer) Connect(connectionstring Config, config *sarama.Config) error {
	if consumer.client != nil {
		err := consumer.client.Close()
		if err != nil {
			return err
		}
	}

	consumer.ready = make(chan bool, 0)

	ctx := context.Background()
	client, err := sarama.NewConsumerGroup(connectionstring.Brokers, connectionstring.Group, config)
	if err != nil {
		return err
	}

	go client.Consume(ctx, consumer.topics, consumer)

	select {
	case err := <-client.Errors():
		commander.Logger.Println(err)
		return err
	case <-consumer.ready:
	}

	consumer.client = client
	consumer.connectionstring = connectionstring
	consumer.config = config

	return nil
}

// Subscribe subscribes to the given topics and returs a message channel
func (consumer *Consumer) Subscribe(topics ...commander.Topic) (<-chan *commander.Message, error) {
	commander.Logger.Println("Subscribing to topics:", topics)

	subscription := make(chan *commander.Message, 1)

	consumer.mutex.RLock()
	defer consumer.mutex.RUnlock()

	for _, topic := range topics {
		consumer.subscriptions[topic.Name] = append(consumer.subscriptions[topic.Name], subscription)
	}

	return subscription, nil
}

// Unsubscribe unsubscribes the given topic from the subscription list
func (consumer *Consumer) Unsubscribe(channel <-chan *commander.Message) error {
	consumer.mutex.RLock()
	defer consumer.mutex.RUnlock()

	for topic, subscriptions := range consumer.subscriptions {
		for index, subscription := range subscriptions {
			if subscription == channel {
				consumer.subscriptions[topic] = append(consumer.subscriptions[topic][:index], consumer.subscriptions[topic][index+1:]...)
				close(subscription)
				break
			}
		}
	}

	return nil
}

// Close closes the kafka consumer
func (consumer *Consumer) Close() error {
	consumer.client.Close()
	consumer.consumptions.Wait()

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for topic, subscriptions := range consumer.subscriptions {
		for _, subscription := range subscriptions {
			close(subscription)
		}

		consumer.subscriptions[topic] = nil
	}

	return nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	consumer.ready = make(chan bool, 0)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		commander.Logger.Println("Message claimed")
		consumer.consumptions.Add(1)

		subscriptions := consumer.subscriptions[message.Topic]
		if len(subscriptions) > 0 {
			headers := map[string]string{}
			for _, record := range message.Headers {
				headers[string(record.Key)] = string(record.Value)
			}

			message := &commander.Message{
				Headers: headers,
				Topic: commander.Topic{
					Name: message.Topic,
				},
				Value:     message.Value,
				Key:       message.Key,
				Timestamp: message.Timestamp,
			}

			for _, subscription := range subscriptions {
				subscription <- message
			}
		}

		session.MarkMessage(message, "")
		consumer.consumptions.Done()
	}

	return nil
}
