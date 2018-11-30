package kafka

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander"
)

// NewConsumer constructs a new kafka dialect consumer
func NewConsumer(client sarama.ConsumerGroup, groups ...*commander.Group) *Consumer {
	ctx := context.Background()
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
		client:        client,
		subscriptions: make(map[string][]chan *commander.Message),
	}

	go client.Consume(ctx, topics, consumer)
	return consumer
}

// Consumer consumes kafka messages
type Consumer struct {
	client        sarama.ConsumerGroup
	subscriptions map[string][]chan *commander.Message
	consumptions  sync.WaitGroup
	mutex         sync.Mutex
}

// Subscribe subscribes to the given topics and returs a message channel
func (consumer *Consumer) Subscribe(topics ...commander.Topic) (<-chan *commander.Message, error) {
	subscription := make(chan *commander.Message, 1)

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for _, topic := range topics {
		consumer.subscriptions[topic.Name] = append(consumer.subscriptions[topic.Name], subscription)
	}

	return subscription, nil
}

// Unsubscribe unsubscribes the given topic from the subscription list
func (consumer *Consumer) Unsubscribe(<-chan *commander.Message) error {
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
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		consumer.consumptions.Add(1)
		consumer.mutex.Lock()

		subscriptions := consumer.subscriptions[message.Topic]
		for _, subscription := range subscriptions {
			headers := []commander.Header{}
			for _, record := range message.Headers {
				header := commander.Header{
					Key:   string(record.Key),
					Value: record.Value,
				}

				headers = append(headers, header)
			}

			message := &commander.Message{
				Headers: headers,
				Topic: commander.Topic{
					Name: message.Topic,
				},
				Value: message.Value,
				Key:   message.Key,
			}

			subscription <- message
		}

		session.MarkMessage(message, "")

		consumer.mutex.Unlock()
		consumer.consumptions.Done()
	}

	return nil
}
