package commander

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	timeout = 10 * time.Second
)

var topic = "commands"

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
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
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
	handle.Start()

	// Wait for event to return
	// A error is thrown if the event does not return within the given period
syncEvent:
	for {
		select {
		case e := <-handle.source:
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
	consumer.SubscribeTopics([]string{"events"}, nil)

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

			handle.source <- event
		}
	}
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
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})

	if err != nil {
		panic(err)
	}

	return producer
}
