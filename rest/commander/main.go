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
	Brokers string
	Group   string
}

// AsyncCommand ...
func (c *Commander) AsyncCommand(command Command) error {
	producer := NewProducer(c.Brokers)
	delivery := make(chan kafka.Event)

	fmt.Println("Sending command:", command)

	defer close(delivery)

	value, _ := json.Marshal(command)
	fmt.Println("Marshal:", string(value))

	err := producer.Produce(&kafka.Message{
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
			if e.Action == command.Action {
				break
			}

			return e, nil
		case <-ctx.Done():
			break syncEvent
		}
	}

	return event, errors.New("timeout reached")
}

// ConsumeEvents ...
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

		events <- event
	}
}

// NewConsumer ...
func NewConsumer(brokers string, group string) *kafka.Consumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          group,
		"auto.offset.reset": "earliest",
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
