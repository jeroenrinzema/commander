package commander

import (
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	// BeforeEvent gets called before a action gets taken.
	BeforeEvent = "before"
	// AfterEvent gets called after a action has been taken.
	AfterEvent = "after"
)

// New creates a new commander instance of the given config
func New(config *Config) *Commander {
	commander := Commander{
		Producer: &Producer{},
		Consumer: &Consumer{},
		closing:  make(chan bool, 1),
	}

	return &commander
}

// Commander contains all information of a commander instance
type Commander struct {
	*Config
	Groups   []*Group
	Producer *Producer
	Consumer *Consumer
	closing  chan bool
	mutex    sync.Mutex
}

// Consume starts consuming messages with the set consumer.
func (commander *Commander) Consume() {
	commander.Consumer.Consume()
}

// AddGroups registeres a commander group and initializes it with
// the set consumer and producer.
func (commander *Commander) AddGroups(groups ...*Group) error {
	commander.mutex.Lock()
	defer commander.mutex.Unlock()

	commander.Groups = append(commander.Groups, groups...)
	err := commander.Consumer.AddGroups(groups...)

	if err != nil {
		return err
	}

	return nil
}

// Produce a new message to kafka. A error will be returnes if something went wrong in the process.
func (commander *Commander) Produce(message *kafka.Message) error {
	return nil
}

// BeforeClosing returns a channel that gets called before the commander
// instance is closed.
func (commander *Commander) BeforeClosing() <-chan bool {
	return commander.closing
}

// BeforeConsuming returns a channel which is called before a messages is
// passed on to a consumer.
func (commander *Commander) BeforeConsuming() {
}

// AfterConsumed returns a channel which is called after a message is consumed.
func (commander *Commander) AfterConsumed() {
}
