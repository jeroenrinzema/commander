package commander

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
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
	// IDHeader kafka message id header
	IDHeader = "key"
	// AcknowledgedHeader kafka message acknowledged header
	AcknowledgedHeader = "acknowledged"
	// VersionHeader kafka message version header
	VersionHeader = "version"
	// AnyTopic is a const given to the Consume method when wanting to consume "any" topic
	AnyTopic = ""
)

// Commander is a struct that contains all required methods
type Commander struct {
	Consumer     *kafka.Consumer
	Producer     *kafka.Producer
	Timeout      time.Duration
	CommandTopic string
	EventTopic   string

	topics    []string
	consumers []*Consumer
	closing   chan bool
	sink      chan kafka.Event
}

// StartConsuming starts consuming kafka events, this method is blocking.
// Once a kafka event is consumed will it be passed on to the listening consumers "Events" channel.
// The consumer is closed once a close signal is given to commander.
func (commander *Commander) StartConsuming() {
	for {
		select {
		case <-commander.BeforeClosing():
			// Optionally could preform some actions before a consumer is closing
			return
		case event := <-commander.Consumer.Events():
			var topic string

			switch message := event.(type) {
			case *kafka.Message:
				topic = *message.TopicPartition.Topic
			case kafka.PartitionEOF:
				topic = *message.Topic
			}

			for _, consumer := range commander.consumers {
				if topic != consumer.Topic && len(consumer.Topic) != 0 {
					continue
				}

				consumer.Events <- event
			}
		}
	}
}

// Consume create and return a new kafka event consumer.
// All received events on the given kafka topic are passed on to the "Events" channel.
// When a empty string as "topic" is given is it seen as a wildcard and will
// all consumed events from all topics be published to the "Events" channel.
func (commander *Commander) Consume(topic string) *Consumer {
	consumer := &Consumer{
		Topic:     topic,
		Events:    make(chan kafka.Event),
		commander: commander,
	}

	if len(topic) > 0 {
		commander.RegisterTopic(topic)
	}

	commander.consumers = append(commander.consumers, consumer)
	return consumer
}

// RegisterTopic registers a new topic for consumption.
// If the topic is already marked as registered will it be ignored.
func (commander *Commander) RegisterTopic(topic string) {
	for _, top := range commander.topics {
		if top == topic {
			return
		}
	}

	log.Println("subscribing to topic", topic)

	commander.topics = append(commander.topics, topic)
	commander.Consumer.SubscribeTopics(commander.topics, nil)
}

// NewEventsConsumer starts consuming the events from the set events topic.
// The returned consumer consumes all events of all actions.
// The topic that gets consumed is set during initialization (commander.EventTopic) of the commander struct.
// All received messages are published over the returned channel.
func (commander *Commander) NewEventsConsumer() (chan *Event, *Consumer) {
	sink := make(chan *Event)
	consumer := commander.Consume(commander.EventTopic)

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

	return sink, consumer
}

// NewEventConsumer starts consuming events of the given action with one of the given version from the set commands topic.
// The topic that gets consumed is set during initialization (commander.EventTopic) of the commander struct.
// All received events are published over the returned go channel.
// The consumer gets closed once a close signal is given to commander.
func (commander *Commander) NewEventConsumer(action string, versions []int) (chan *Event, *Consumer) {
	sink := make(chan *Event)
	consumer := commander.Consume(commander.EventTopic)

	go func() {
		for {
			select {
			case <-consumer.BeforeClosing():
				close(sink)
				return
			case event := <-consumer.Events:
				switch message := event.(type) {
				case *kafka.Message:
					var messageAction string
					versionMatch := false

					for _, header := range message.Headers {
						if header.Key == ActionHeader {
							messageAction = string(header.Value)
							break
						}
					}

					if messageAction != action {
						break
					}

					event := Event{}
					event.Populate(message)

					for _, version := range versions {
						if version == event.Version {
							versionMatch = true
							break
						}
					}

					if !versionMatch {
						break
					}

					sink <- &event
				}
			}
		}
	}()

	return sink, consumer
}

// NewCommandsConsumer starts consuming commands from the set commands topic.
// The returned consumer consumes all commands of all actions.
// The topic that gets consumed is set during initialization (commander.CommandTopic) of the commander struct.
// All received messages are send over the returned channel.
func (commander *Commander) NewCommandsConsumer() (chan *Command, *Consumer) {
	sink := make(chan *Command)
	consumer := commander.Consume(commander.CommandTopic)

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

	return sink, consumer
}

// NewCommandConsumer starts consuming commands of the given action from the set commands topic.
// The topic that gets consumed is set during initialization (commander.CommandTopic) of the commander struct.
// All received messages are send over the returned channel.
func (commander *Commander) NewCommandConsumer(action string) (chan *Command, *Consumer) {
	sink := make(chan *Command)
	consumer := commander.Consume(commander.CommandTopic)

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

	return sink, consumer
}

// CommandHandle is a callback function used to handle/process commands
type CommandHandle func(*Command)

// NewCommandHandle is a small wrapper around NewCommandConsumer that awaits till the given action is received.
// Once a command of the given action is received is the CommandHandle callback function called.
// The handle is closed once the consumer receives a close signal.
func (commander *Commander) NewCommandHandle(action string, callback CommandHandle) func() {
	commands, consumer := commander.NewCommandConsumer(action)

	go func() {
		for {
			select {
			case <-consumer.closing:
				break
			case command := <-commands:
				callback(command)
			}
		}
	}()

	return consumer.Close
}

// EventHandle is a callback function used to handle/process events
type EventHandle func(*Event)

// NewEventHandle is a small wrapper around NewEventConsumer that awaits till the given event is received.
// Once a event of the given action is received is the EventHandle callback called.
// The handle is closed once the consumer receives a close signal.
func (commander *Commander) NewEventHandle(action string, versions []int, callback EventHandle) func() {
	commands, consumer := commander.NewEventConsumer(action, versions)

	go func() {
		for {
			select {
			case <-consumer.closing:
				break
			case command := <-commands:
				callback(command)
			}
		}
	}()

	return consumer.Close
}

// Produce a new message to kafka.
// A error is returned if anything went wrong in the process.
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

	log.Println("producing message into topic", message.TopicPartition.Topic)
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

// ProduceCommand produces a new command message to the set commands topic
func (commander *Commander) ProduceCommand(command *Command) error {
	message := kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   "action",
				Value: []byte(command.Action),
			},
		},
		Key:            command.ID.Bytes(),
		TopicPartition: kafka.TopicPartition{Topic: &commander.CommandTopic, Partition: kafka.PartitionAny},
		Value:          command.Data,
	}

	log.Println("producing command with action:", command.Action)
	return commander.Produce(&message)
}

// SyncCommand produces a new command and waits for the resulting event.
// If the resulting event is not created within the set timeout period will a timeout error be returned.
func (commander *Commander) SyncCommand(command *Command) (*Event, error) {
	err := commander.AsyncCommand(command)

	if err != nil {
		return nil, err
	}

	events, consumer := commander.NewEventsConsumer()
	ctx, cancel := context.WithTimeout(context.Background(), commander.Timeout)

	defer consumer.Close()
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

// ProduceEvent produces a new event message to the set events topic.
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
				Key:   IDHeader,
				Value: event.ID.Bytes(),
			},
			kafka.Header{
				Key:   AcknowledgedHeader,
				Value: []byte(strconv.FormatBool(event.Acknowledged)),
			},
			kafka.Header{
				Key:   VersionHeader,
				Value: []byte(strconv.Itoa(event.Version)),
			},
		},
		Key:            event.Key.Bytes(),
		TopicPartition: kafka.TopicPartition{Topic: &commander.EventTopic},
		Value:          event.Data,
	}

	log.Println("producing event with action:", event.Action)
	return commander.Produce(message)
}

// BeforeClosing returns a channel that gets published a boolean to before commander gets closed.
func (commander *Commander) BeforeClosing() chan bool {
	if commander.closing == nil {
		commander.closing = make(chan bool)
	}

	return commander.closing
}

// Close the commander consumers, producers and other processes.
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

// CloseOnSIGTERM starts a go routine that closes this commander instance once a SIGTERM signal is send to the process.
func (commander *Commander) CloseOnSIGTERM() {
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		<-sigs
		commander.Close()
		os.Exit(0)
	}()
}

// NewProducer creates a new kafka produces but panics if something went wrong.
// A kafka config map could be given with additional settings.
func NewProducer(conf *kafka.ConfigMap) *kafka.Producer {
	producer, err := kafka.NewProducer(conf)

	if err != nil {
		panic(err)
	}

	return producer
}

// NewConsumer creates a kafka consumer but panics if something went wrong.
// A kafka config map could be given with additional settings.
// The option "go.events.channel.enable" is set to true for commander to function optimally.
func NewConsumer(conf *kafka.ConfigMap) *kafka.Consumer {
	conf.SetKey("go.events.channel.enable", true)
	consumer, err := kafka.NewConsumer(conf)

	if err != nil {
		panic(err)
	}

	return consumer
}

// NewCommand create a new command with the given action and data.
// A unique ID is generated and set in order to trace the command.
func NewCommand(action string, data []byte) *Command {
	id := uuid.NewV4()

	command := Command{
		ID:     id,
		Action: action,
		Data:   data,
	}

	return &command
}
