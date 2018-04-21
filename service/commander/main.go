package commander

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// CommandHandle ...
type CommandHandle func(Command)

var (
	commands = make(chan Command)
	consume  = "commander"
	produce  = "events"
)

// Commander ...
type Commander struct {
	Brokers  string
	Group    string
	Producer *kafka.Producer
}

// ConsumeCommands ...
func (c *Commander) ConsumeCommands() {
	consumer := NewConsumer(c.Brokers, c.Group)
	consumer.SubscribeTopics([]string{"commands"}, nil)

	go func() {
		for {
			msg, err := consumer.ReadMessage(-1)

			if err != nil {
				panic(err)
			}

			command := Command{}
			json.Unmarshal(msg.Value, &command)

			commands <- command
		}
	}()
}

// NewEvent ...
func (c *Commander) NewEvent(event Event) error {
	producer := NewProducer(c.Brokers)
	delivery := make(chan kafka.Event)

	defer close(delivery)
	defer producer.Close()

	value, _ := json.Marshal(event)
	err := producer.Produce(&kafka.Message{
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

// CommandHandle ...
func (c *Commander) CommandHandle(action string, callback CommandHandle) {
	for command := range commands {
		if command.Action != action {
			continue
		}

		callback(command)
	}
}

// OpenProducer ...
func (c *Commander) OpenProducer() {
	c.Producer = NewProducer(c.Brokers)
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
