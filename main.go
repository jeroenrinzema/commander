package commander

import (
	"sync"

	"github.com/Shopify/sarama"
)

// New creates a new commander instance of the given config
func New(config *Config) {

}

// Commander contains all information of a commander instance
type Commander struct {
	Config   *Config
	Producer *Producer
	Consumer *Consumer
	Groups   []*Group

	mutex   sync.Mutex
	closing chan bool
}

// Consume starts consuming messages with the set consumer.
func (commander *Commander) Consume() {
}

// Produce a new message to kafka. A error will be returnes if something went wrong in the process.
func (commander *Commander) Produce(message *sarama.ProducerMessage) error {
	return nil
}

// BeforeClosing the channel returned is called before the commander
// instance is closed.
func (commander *Commander) BeforeClosing() {
}

// BeforeConsuming returns a channel which is called before a messages is
// passed on to a consumer.
func (commander *Commander) BeforeConsuming() {
}

// AfterConsumed returns a channel which is called after a message is consumed.
func (commander *Commander) AfterConsumed() {
}
