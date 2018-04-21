package commander

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

const (
	timeout = 10 * time.Second
)

var (
	commandsTopic = "commands"
	eventsTopic   = "events"
)

// Server a variable that stores the current commander server
var Server *Commander

// Config used to create a new commander server
type Config struct {
	Brokers string
	Group   string
}

// NewServer create a new commander server
func NewServer(config *Config) *Commander {
	Server = &Commander{
		Brokers: config.Brokers,
		Group:   config.Group,
	}

	return Server
}

// Commander create a new commander instance
type Commander struct {
	Brokers  string
	Group    string
	Producer *kafka.Producer
}

// AsyncCommand create a async command.
// This method will deliver the command to the commands topic but will not wait for a response event.
func (c *Commander) AsyncCommand(command Command) error {
	delivery := make(chan kafka.Event)
	defer close(delivery)

	value, _ := json.Marshal(command)

	err := c.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &commandsTopic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
	}, delivery)

	if err != nil {
		panic(err)
	}

	response := <-delivery
	message := response.(*kafka.Message)

	fmt.Println("delivered message,", command.Action, time.Now())

	if message.TopicPartition.Error != nil {
		return message.TopicPartition.Error
	}

	return nil
}

// SyncCommand send a synchronized command.
// This method will wait till a response event is given.
func (c *Commander) SyncCommand(command Command) (Event, error) {
	err := c.AsyncCommand(command)
	event := Event{}

	if err != nil {
		return event, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	handle := Handle{ID: command.ID}
	handle.Listen()

	// Wait for event to return
	// A error is thrown if the event does not return within the given period
syncEvent:
	for {
		select {
		case e := <-handle.event:
			handle.Close()
			return e, nil
		case <-ctx.Done():
			break syncEvent
		}
	}

	return event, errors.New("timeout reached")
}

// OpenProducer open a new kafka producer and store it in the struct
func (c *Commander) OpenProducer() {
	c.Producer = NewProducer(c.Brokers)
}

// ConsumeEvents creates a new consumer and starts listening on the events Topic
// When a new event message is received is checked if the command exists on the handles slice.
// If the command is found will a message be send over the source channel.
func (c *Commander) ConsumeEvents() {
	consumer := NewConsumer(c.Brokers, c.Group)
	consumer.SubscribeTopics([]string{eventsTopic}, nil)

	for {
		msg, err := consumer.ReadMessage(-1)

		if err != nil {
			panic(err)
		}

		event := Event{}
		json.Unmarshal(msg.Value, &event)

		for _, handle := range handles {
			if handle.ID != event.Parent {
				continue
			}

			handle.event <- event
		}
	}
}

// ConsumeCommands creates a new consumer and starts listening on the commands Topic
// When a new command message is received is checked if the command exists on the handles slice.
// If the command is found will a message be send over the source channel.
func (c *Commander) ConsumeCommands() {
	consumer := NewConsumer(c.Brokers, c.Group)
	consumer.SubscribeTopics([]string{"commands"}, nil)

	for {
		msg, err := consumer.ReadMessage(-1)

		if err != nil {
			panic(err)
		}

		command := Command{}
		json.Unmarshal(msg.Value, &command)

		fmt.Println("Received command,", command.Action)

		for _, handle := range handles {
			if handle.Action != command.Action {
				continue
			}

			handle.command <- command
		}
	}
}

// NewEvent send a new event to the event kafka topic
func (c *Commander) NewEvent(event Event) error {
	delivery := make(chan kafka.Event)
	defer close(delivery)

	value, _ := json.Marshal(event)
	err := c.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &eventsTopic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
	}, delivery)

	if err != nil {
		panic(err)
	}

	response := <-delivery
	message := response.(*kafka.Message)

	if message.TopicPartition.Error != nil {
		return message.TopicPartition.Error
	}

	return nil
}

// NewCommand create a new command with the given action and data
func NewCommand(action string, data interface{}) Command {
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

// NewProducer create a new kafka producer that connects to the given brokers
func NewProducer(brokers string) *kafka.Producer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})

	if err != nil {
		panic(err)
	}

	return producer
}
