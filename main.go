package commander

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

// CommandCallback the callback function that is called when a command message is received
type CommandCallback func(command *Command)

const (
	timeout = 5 * time.Second
)

var (
	// CommandsTopic the kafka commands topic
	CommandsTopic = "commands"
	// EventsTopic the kafka events topic
	EventsTopic = "events"
)

// Event ...
type Event struct {
	Parent    uuid.UUID       `json:"parent"`
	ID        uuid.UUID       `json:"id"`
	Action    string          `json:"action"`
	Data      json.RawMessage `json:"data"`
	commander *Commander
}

// Produce the created event
func (e *Event) Produce() {
	e.commander.ProduceEvent(e)
}

// Command ...
type Command struct {
	ID        uuid.UUID       `json:"id"`
	Action    string          `json:"action"`
	Data      json.RawMessage `json:"data"`
	commander *Commander
}

// NewEvent create a new command with the given action and data
func (c *Command) NewEvent(action string, key uuid.UUID, data []byte) Event {
	id, _ := uuid.NewV4()

	event := Event{
		Parent:    c.ID,
		ID:        id,
		Action:    action,
		Data:      data,
		commander: c.commander,
	}

	return event
}

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
			// Collect the header values
			for _, header := range msg.Headers {
				if header.Key == "action" {
					event.Action = string(header.Value)
				}

				if header.Key == "parent" {
					parent, err := uuid.FromBytes(header.Value)

					if err != nil {
						continue syncEvent
					}

					event.Parent = parent
				}
			}

			if event.Parent != command.ID {
				continue
			}

			id, err := uuid.FromBytes(msg.Key)

			if err != nil {
				continue
			}

			event.ID = id
			event.Data = msg.Value

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
				Key:   "action",
				Value: []byte(event.Action),
			},
			kafka.Header{
				Key:   "parent",
				Value: event.Parent.Bytes(),
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

// Handle call the callback function if the given command is received
func (c *Commander) Handle(action string, callback CommandCallback) {
	consumer := c.Consume(CommandsTopic)

	go func() {
		defer consumer.Close()

		for msg := range consumer.Messages {
			command := Command{
				commander: c,
			}

			// Collect the header values
			for _, header := range msg.Headers {
				if header.Key == "action" {
					command.Action = string(header.Value)
				}
			}

			if action != command.Action {
				continue
			}

			id, err := uuid.FromBytes(msg.Key)

			if err != nil {
				continue
			}

			command.ID = id
			command.Data = msg.Value
			callback(&command)
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
	id, _ := uuid.NewV4()

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
