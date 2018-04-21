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

var (
	events = make(chan Event)
	topic  = "commands"
)

// Commander ...
type Commander struct {
	Brokers  string
	Group    string
	Producer *kafka.Producer
}

// AsyncCommand ...
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

	if message.TopicPartition.Error != nil {
		return message.TopicPartition.Error
	}

	return nil
}

// SyncCommand ...
func (c *Commander) SyncCommand(command Command) (Event, error) {
	fmt.Println("Sending:", command)
	err := c.AsyncCommand(command)
	event := Event{}

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
		case e := <-events:
			fmt.Println("event")
			if e.Parent == command.ID {
				fmt.Println("match")
				return e, nil
			}
		case <-ctx.Done():
			break syncEvent
		}
	}

	return event, errors.New("timeout reached")
}

// OpenProducer ...
func (c *Commander) OpenProducer() {
	c.Producer = NewProducer(c.Brokers)
}

// ConsumeEvents ...
func (c *Commander) ConsumeEvents() {
	consumer := NewConsumer(c.Brokers, c.Group)
	consumer.SubscribeTopics([]string{"events"}, nil)

	go func() {
		for {
			msg, err := consumer.ReadMessage(-1)

			if err != nil {
				panic(err)
			}

			fmt.Println("received:", string(msg.Value))

			event := Event{}
			json.Unmarshal(msg.Value, &event)

			events <- event
		}
	}()
}

// NewConsumer ...
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

// NewProducer ...
func NewProducer(brokers string) *kafka.Producer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})

	if err != nil {
		panic(err)
	}

	return producer
}
