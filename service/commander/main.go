package commander

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	commands = make(chan Command)
	consume  = "commander"
	produce  = "events"
)

// Commander create a new commander instance
type Commander struct {
	Brokers  string
	Group    string
	Producer *kafka.Producer
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

			handle.source <- command
		}
	}
}

// NewEvent send a new event to the event kafka topic
func (c *Commander) NewEvent(event Event) error {
	delivery := make(chan kafka.Event)
	defer close(delivery)

	value, _ := json.Marshal(event)
	err := c.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &produce, Partition: kafka.PartitionAny},
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

// OpenProducer open a new kafka producer and store it in the struct
func (c *Commander) OpenProducer() {
	c.Producer = NewProducer(c.Brokers)
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
