package commander

import (
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	// BeforeEvent gets called before a action gets taken.
	BeforeEvent = "before"
	// AfterEvent gets called after a action has been taken.
	AfterEvent = "after"
)

// New creates a new commander instance of the given config
func New(config *Config) (Client, error) {
	var producer Producer
	var consumer Consumer
	var err error

	config.Kafka.SetKey("group.id", config.Group)
	config.Kafka.SetKey("bootstrap.servers", strings.Join(config.Brokers, ","))

	producer, err = NewProducer(config.Kafka)
	if err != nil {
		return nil, err
	}

	consumer, err = NewConsumer(config.Kafka)
	if err != nil {
		return nil, err
	}

	client := &client{
		config:   config,
		producer: producer,
		consumer: consumer,
		closing:  make(chan bool, 1),
	}

	for _, group := range config.Groups {
		group.Client = client
	}

	consumer.AddGroups(config.Groups...)
	return client, nil
}

// Client manages the consumers, producers and groups.
type Client interface {
	// Consume starts consuming messages with the set consumer.
	Consume()

	// Consumer returns the clients consumer
	Consumer() Consumer

	// Producer returns the clients producer
	Producer() Producer

	// Produce a new message to kafka. A error will be returnes if something went wrong in the process.
	Produce(message *kafka.Message) error

	// BeforeClosing returns a channel that gets called before the commander
	// instance is closed.
	BeforeClosing() <-chan bool

	// BeforeConsuming returns a channel which is called before a messages is
	// passed on to a consumer. Two arguments are returned. The events channel and a closing function.
	BeforeConsuming(ConsumerEventHandle) func()

	// AfterConsumed returns a channel which is called after a message is consumed.
	// Two arguments are returned. The events channel and a closing function.
	AfterConsumed(ConsumerEventHandle) func()

	// CloseOnSIGTERM automatically closes the commander instance
	// once the SIGTERM signal is send.
	CloseOnSIGTERM()

	// Close closes the kafka consumer and finishes the last consumed messages
	Close()
}

type client struct {
	config   *Config
	groups   []*Group
	producer Producer
	consumer Consumer
	closing  chan bool
}

func (client *client) Consumer() Consumer {
	return client.consumer
}

func (client *client) Producer() Producer {
	return client.producer
}

func (client *client) Consume() {
	client.consumer.Consume()
}

func (client *client) Produce(message *kafka.Message) error {
	return nil
}

func (client *client) BeforeClosing() <-chan bool {
	return client.closing
}

func (client *client) BeforeConsuming(handle ConsumerEventHandle) func() {
	return client.consumer.OnEvent(BeforeEvent, handle)
}

func (client *client) AfterConsumed(handle ConsumerEventHandle) func() {
	return client.consumer.OnEvent(AfterEvent, handle)
}

func (client *client) CloseOnSIGTERM() {
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm
	client.Close()
}

func (client *client) Close() {
	client.Consumer().Close()
}
