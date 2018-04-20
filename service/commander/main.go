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

// Commander ...
type Commander struct {
	Brokers string
	Group   string
}

// ConsumeCommands ...
func (c *Commander) ConsumeCommands() {
	consumer := NewConsumer(c.Brokers, c.Group)

	consumer.SubscribeTopics([]string{"commands"}, nil)

	for {
		msg, err := consumer.ReadMessage(-1)

		fmt.Println(msg)

		if err != nil {
			panic(err)
		}

		command := Command{}
		json.Unmarshal(msg.Value, &command)
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
