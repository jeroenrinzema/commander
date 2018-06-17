package commander

import (
	"context"
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

// CommandCallback the callback function that is called when a command message is received
type CommandCallback func(command *Command)

// EventCallback the callback function that is called when a event message is received
type EventCallback func(event *Event)

const (
	timeout = 5 * time.Second

	// OperationHeader ...
	OperationHeader = "operation"
	// ParentHeader ...
	ParentHeader = "parent"
	// ActionHeader ...
	ActionHeader = "action"
	// KeyHeader ...
	KeyHeader = "key"
)

var (
	// CommandsTopic the kafka commands topic
	CommandsTopic = "commands"
	// EventsTopic the kafka events topic
	EventsTopic = "events"
)

// Commander create a new commander instance
type Commander struct {
	Producer *kafka.Producer
	Consumer *kafka.Consumer
}

// Produce a new kafka message
func (c *Commander) Produce(message *kafka.Message) error {
	delivery := make(chan kafka.Event)
	defer close(delivery)

	err := c.Producer.Produce(message, delivery)

	if err != nil {
		return err
	}

	response := <-delivery
	value := response.(*kafka.Message)

	if value.TopicPartition.Error != nil {
		return value.TopicPartition.Error
	}

	return nil
}

// AsyncCommand create a async command.
// This method will deliver the command to the commands topic but will not wait for a response event.
func (c *Commander) AsyncCommand(command Command) error {
	message := &kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   "action",
				Value: []byte(command.Action),
			},
		},
		Key:            command.ID.Bytes(),
		TopicPartition: kafka.TopicPartition{Topic: &CommandsTopic, Partition: kafka.PartitionAny},
		Value:          command.Data,
	}

	return c.Produce(message)
}

// SyncCommand send a synchronized command.
// This method will wait till a response event is given.
func (c *Commander) SyncCommand(command Command) (Event, error) {
	err := c.AsyncCommand(command)
	consumer := c.Consume(EventsTopic)
	event := Event{}

	defer consumer.Close()

	if err != nil {
		return event, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Wait for event to return
	// A error is thrown if the event does not return within the given period
syncEvent:
	for {
		select {
		case msg := <-consumer.Messages:
			err := event.Populate(msg)

			if err != nil {
				continue syncEvent
			}

			if event.Parent != command.ID {
				continue
			}

			return event, nil
		case <-ctx.Done():
			break syncEvent
		}
	}

	return event, errors.New("timeout reached")
}

// ProduceEvent produce a new event to the events topic
func (c *Commander) ProduceEvent(event *Event) error {
	message := &kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(event.Action),
			},
			kafka.Header{
				Key:   ParentHeader,
				Value: event.Parent.Bytes(),
			},
			kafka.Header{
				Key:   OperationHeader,
				Value: []byte(event.Operation),
			},
			kafka.Header{
				Key:   KeyHeader,
				Value: event.Key.Bytes(),
			},
		},
		Key:            event.ID.Bytes(),
		TopicPartition: kafka.TopicPartition{Topic: &EventsTopic},
		Value:          event.Data,
	}

	return c.Produce(message)
}

// Consume create a new kafka consumer if no consumer for the given topic exists
func (c *Commander) Consume(topic string) *Consumer {
	messages := make(chan *kafka.Message)
	consumer := &Consumer{
		Commander: c,
		Topic:     topic,
		Messages:  messages,
	}

	consumer.Read()
	return consumer
}

// HandleCommand call the callback function if the given command is received
func (c *Commander) HandleCommand(action string, callback CommandCallback) {
	consumer := c.Consume(CommandsTopic)

	go func() {
		defer consumer.Close()

		for msg := range consumer.Messages {
			command := Command{
				commander: c,
			}

			err := command.Populate(msg)

			if err != nil {
				continue
			}

			if action != command.Action {
				continue
			}

			callback(&command)
		}
	}()
}

// HandleEvent call the callback function if a event is received
func (c *Commander) HandleEvent(callback EventCallback) {
	consumer := c.Consume(EventsTopic)

	go func() {
		defer consumer.Close()

		for msg := range consumer.Messages {
			event := Event{}
			err := event.Populate(msg)

			if err != nil {
				continue
			}

			callback(&event)
		}
	}()
}

// ReadMessages start consuming all messages
func (c *Commander) ReadMessages() {
	for {
		msg, err := c.Consumer.ReadMessage(-1)

		if err != nil {
			panic(err)
		}

		topic := *msg.TopicPartition.Topic

		for _, consumer := range consumers {
			if consumer.Topic == topic {
				consumer.Messages <- msg
			}
		}
	}
}

// Close the commander consumer and producer
func (c *Commander) Close() {
	c.Consumer.Close()
	c.Producer.Close()
}

// NewCommand create a new command with the given action and data
func NewCommand(action string, data []byte) Command {
	id := uuid.NewV4()

	command := Command{
		ID:     id,
		Action: action,
		Data:   data,
	}

	return command
}

// NewConsumer create a new kafka consumer that connects to the given brokers and group
func NewConsumer(brokers string, group string) *kafka.Consumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          group,
		// "auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	return consumer
}

// NewProducer create a new kafka Producer that connects to the given brokers
func NewProducer(brokers string) *kafka.Producer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})

	if err != nil {
		panic(err)
	}

	return producer
}
