package commander

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	// BeforeEvent gets called before a action gets taken.
	BeforeEvent = "before"
	// AfterEvent gets called after a action has been taken.
	AfterEvent = "after"
)

// New creates a new commander instance of the given config
func New(config Config) (Client, error) {
	var producer Producer
	var consumer Consumer
	var err error

	producer, err = NewProducer(config.Kafka)
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	consumer, err = NewConsumer(config.Kafka)
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	client := &client{
		Config:   config,
		producer: producer,
		consumer: consumer,
		closing:  make(chan bool, 1),
	}

	return client, nil
}

// Client manages the consumers, producers and groups.
type Client interface {
	// Consume starts consuming messages with the set consumer.
	Consume()

	// Consumer returns the clients consumer
	Consumer() Consumer

	// Producer returns the clients producer
	Producer()

	// Produce a new message to kafka. A error will be returnes if something went wrong in the process.
	Produce(message *kafka.Message) error

	// BeforeClosing returns a channel that gets called before the commander
	// instance is closed.
	BeforeClosing() <-chan bool

	// BeforeConsuming returns a channel which is called before a messages is
	// passed on to a consumer. Two arguments are returned. The events channel and a closing function.
	BeforeConsuming() (<-chan kafka.Event, func())

	// AfterConsumed returns a channel which is called after a message is consumed.
	// Two arguments are returned. The events channel and a closing function.
	AfterConsumed() (<-chan kafka.Event, func())
}

type client struct {
	Config
	groups   []*Group
	producer Producer
	consumer Consumer
	closing  chan bool
}

func (client *client) Consumer() Consumer {
	return client.consumer
}

func (client *client) Producer() {}

func (client *client) Consume() {
	client.consumer.Consume()
}

func (client *client) Produce(message *kafka.Message) error {
	return nil
}

func (client *client) BeforeClosing() <-chan bool {
	return client.closing
}

func (client *client) BeforeConsuming() (<-chan kafka.Event, func()) {
	return client.consumer.OnEvent(BeforeEvent)
}

func (client *client) AfterConsumed() (<-chan kafka.Event, func()) {
	return client.consumer.OnEvent(AfterEvent)
}
