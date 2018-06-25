package commander

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

const (
	// ParentHeader kafka message parent header
	ParentHeader = "parent"
	// ActionHeader kafka message action header
	ActionHeader = "action"
	// KeyHeader kafka message key header
	KeyHeader = "key"
)

var (
	// Timeout is the time that a sync command maximum can take
	Timeout = 5 * time.Second
	// CommandTopic this value is used to mark a topic for command consumption
	CommandTopic = "commanding"
	// EventTopic this value is used to mark a topic for event consumption
	EventTopic = "events"
)

// Commander is a struct that contains all required methods
type Commander struct {
	Consumer *kafka.Consumer
	Producer *kafka.Producer

	topics    []string
	consumers []*Consumer
	closing   chan bool
}

// StartConsuming starts a new go routine that consumes all kafka events.
// All kafka events are pushed into the events commander channel.
func (commander *Commander) StartConsuming() chan bool {
	closing := make(chan bool)
	go func() {
		for {
			select {
			case <-commander.BeforeClosing():
				close(closing)
				return
			case <-closing:
				// Optionally could we preform some actions before a consumer is closing
				return
			case event := <-commander.Consumer.Events():
				switch message := event.(type) {
				case kafka.AssignedPartitions:
					commander.Consumer.Assign(message.Partitions)
				case kafka.RevokedPartitions:
					commander.Consumer.Unassign()
				case *kafka.Message:
					for _, consumer := range commander.consumers {
						if *message.TopicPartition.Topic != consumer.Topic {
							continue
						}

						consumer.Events <- event
					}
				case kafka.PartitionEOF:
					for _, consumer := range commander.consumers {
						if *message.Topic != consumer.Topic {
							continue
						}

						consumer.Events <- event
					}
				}
			}
		}
	}()

	return closing
}

// Consume create a new kafka event consumer
func (commander *Commander) Consume(topic string) *Consumer {
	consumer := &Consumer{
		Topic:     topic,
		Events:    make(chan kafka.Event),
		commander: commander,
	}

	commander.RegisterTopic(topic)
	commander.consumers = append(commander.consumers, consumer)
	return consumer
}

// RegisterTopic registers a new topic for consumption
func (commander *Commander) RegisterTopic(topic string) {
	for _, top := range commander.topics {
		if top == topic {
			return
		}
	}

	commander.topics = append(commander.topics, topic)
	commander.Consumer.SubscribeTopics(commander.topics, nil)
}

// NewEventsConsumer starts consuming the events from the events topic.
// The default events topic is "events", the used topic can be configured during initialization.
// All received messages are send over the "Event" channel.
func (commander *Commander) NewEventsConsumer() (chan *Event, func()) {
	sink := make(chan *Event)
	consumer := commander.Consume(EventTopic)

	go func() {
		for {
			select {
			case <-consumer.BeforeClosing():
				close(sink)
				return
			case event := <-consumer.Events:
				switch message := event.(type) {
				case *kafka.Message:
					event := Event{}
					event.Populate(message)
					sink <- &event
				}
			}
		}
	}()

	return sink, consumer.Close
}

// NewCommandsConsumer starts consuming commands from the commands topic.
// The default commands topic is "commands", the used topic can be configured during initialization.
// All received messages are send over the "commands" channel.
func (commander *Commander) NewCommandsConsumer() (chan *Command, func()) {
	sink := make(chan *Command)
	consumer := commander.Consume(CommandTopic)

	go func() {
		for {
			select {
			case <-consumer.BeforeClosing():
				close(sink)
				return
			case event := <-consumer.Events:
				switch message := event.(type) {
				case *kafka.Message:
					command := Command{}
					command.Populate(message)
					sink <- &command
				}
			}
		}
	}()

	return sink, consumer.Close
}

// NewCommandConsumer starts consuming commands with the given action from the commands topic.
// The default commands topic is "commands", the used topic can be configured during initialization.
// All received messages are send over the "commands" channel.
func (commander *Commander) NewCommandConsumer(action string) (chan *Command, func()) {
	sink := make(chan *Command)
	consumer := commander.Consume(CommandTopic)

	go func() {
		for {
			select {
			case <-consumer.BeforeClosing():
				close(sink)
				return
			case event := <-consumer.Events:
				switch message := event.(type) {
				case *kafka.Message:
					match := false
					for _, header := range message.Headers {
						if header.Key == ActionHeader && string(header.Value) == action {
							match = true
						}
					}

					if !match {
						continue
					}

					command := Command{}
					command.Populate(message)
					sink <- &command
				}
			}
		}
	}()

	return sink, consumer.Close
}

// CommandHandle is a callback function that is called upon a action command
type CommandHandle func(*Command)

// NewCommandHandle is a small wrapper around NewCommandConsumer that awaits till the given action is received.
// Once the action is received is the CommandHandle callback called.
func (commander *Commander) NewCommandHandle(action string, callback CommandHandle) func() {
	commands, close := commander.NewCommandConsumer(action)

	go func() {
		for {
			command := <-commands
			callback(command)
		}
	}()

	return close
}

// Produce a new kafka message
func (commander *Commander) Produce(message *kafka.Message) error {
	done := make(chan error)

	go func() {
		defer close(done)
		for event := range commander.Producer.Events() {
			switch event := event.(type) {
			case *kafka.Message:
				if event.TopicPartition.Error != nil {
					done <- event.TopicPartition.Error
					return
				}

				done <- nil
				return
			}
		}
	}()

	commander.Producer.ProduceChannel() <- message
	err := <-done

	if err != nil {
		return err
	}

	return nil
}

// AsyncCommand produces a new command but does not wait on the resulting event.
// A async command is usefull for when you are not interested in the result or the command takes too long to wait for.
func (commander *Commander) AsyncCommand(command *Command) error {
	return commander.ProduceCommand(command)
}

// ProduceCommand produces a new command message to the defined commands topic
func (commander *Commander) ProduceCommand(command *Command) error {
	message := kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   "action",
				Value: []byte(command.Action),
			},
		},
		Key:            command.ID.Bytes(),
		TopicPartition: kafka.TopicPartition{Topic: &CommandTopic, Partition: kafka.PartitionAny},
		Value:          command.Data,
	}

	return commander.Produce(&message)
}

// SyncCommand produces a new command and waits for the resulting event.
func (commander *Commander) SyncCommand(command *Command) (*Event, error) {
	err := commander.AsyncCommand(command)

	if err != nil {
		return nil, err
	}

	events, stop := commander.NewEventsConsumer()
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)

	defer stop()
	defer cancel()

	// Wait for event to return
	// A error is thrown if the event does not return within the given period
syncEvent:
	for {
		select {
		case event := <-events:
			if event.Parent != command.ID {
				continue
			}

			return event, nil
		case <-ctx.Done():
			break syncEvent
		}
	}

	return nil, errors.New("request timeout")
}

// ProduceEvent produces a new event message to the defined events topic
func (commander *Commander) ProduceEvent(event *Event) error {
	message := &kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(event.Action),
			},
			kafka.Header{
				Key:   ParentHeader,
				Value: event.Parent.Bytes(),
			},
			kafka.Header{
				Key:   KeyHeader,
				Value: event.Key.Bytes(),
			},
		},
		Key:            event.ID.Bytes(),
		TopicPartition: kafka.TopicPartition{Topic: &EventTopic},
		Value:          event.Data,
	}

	return commander.Produce(message)
}

// BeforeClosing returns a channel that gets called before commander gets closed
func (commander *Commander) BeforeClosing() chan bool {
	if commander.closing == nil {
		commander.closing = make(chan bool)
	}

	return commander.closing
}

// Close the commander consumer and producer
func (commander *Commander) Close() {
	if commander.closing != nil {
		close(commander.closing)
	}

	for _, consumer := range commander.consumers {
		consumer.Close()
	}

	commander.Producer.Close()
	commander.Consumer.Close()
}

// CloseOnSIGTERM starts a go routine that closes this commander instance once a SIGTERM signal is send
func (commander *Commander) CloseOnSIGTERM() {
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		<-sigs
		commander.Close()
		os.Exit(0)
	}()
}

// NewProducer creates a new kafka produces but panics if something goes wrong
func NewProducer(conf *kafka.ConfigMap) *kafka.Producer {
	producer, err := kafka.NewProducer(conf)

	if err != nil {
		panic(err)
	}

	return producer
}

// NewConsumer creates a kafka consumer but panics if something goes wrong
func NewConsumer(conf *kafka.ConfigMap) *kafka.Consumer {
	conf.SetKey("go.events.channel.enable", true)
	consumer, err := kafka.NewConsumer(conf)

	if err != nil {
		panic(err)
	}

	return consumer
}

// NewCommand create a new command with the given action and data
func NewCommand(action string, data []byte) *Command {
	id := uuid.NewV4()

	command := Command{
		ID:     id,
		Action: action,
		Data:   data,
	}

	return &command
}
