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
	// KeyHeader kafka message key header
	KeyHeader = "key"
	// AcknowledgedHeader kafka message acknowledged header
	AcknowledgedHeader = "acknowledged"
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

// StartConsuming starts a new go routine that consumes all kafka events.
// All kafka events are pushed into the events commander channel.
func (commander *Commander) StartConsuming() {
	for {
		select {
		case <-commander.BeforeClosing():
			// Optionally could we preform some actions before a consumer is closing
			return
		// TODO: add methods to make events consumption plausible
		case event := <-commander.Consumer.Events():
			switch message := event.(type) {
			case *kafka.Message:
				log.Println("received message from topic", *message.TopicPartition.Topic)
				for _, consumer := range commander.consumers {
					if *message.TopicPartition.Topic != consumer.Topic {
						continue
					}

					consumer.Events <- event
				}
			case kafka.PartitionEOF:
				log.Println("reached end of partition", *message.Topic)
				for _, consumer := range commander.consumers {
					if *message.Topic != consumer.Topic {
						continue
					}

					consumer.Events <- event
				}
			}

			if commander.sink != nil {
				commander.sink <- event
			}
		}
	}
}

// Events creates a channel that gets called once a kafka event is received
func (commander *Commander) Events() chan kafka.Event {
	if commander.sink == nil {
		commander.sink = make(chan kafka.Event)
	}

	return commander.sink
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

	log.Println("subscribing to topic", topic)

	commander.topics = append(commander.topics, topic)
	commander.Consumer.SubscribeTopics(commander.topics, nil)
}

// NewEventsConsumer starts consuming the events from the events topic.
// The default events topic is "events", the used topic can be configured during initialization.
// All received messages are send over the returned channel.
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

// NewEventConsumer starts consuming events with of the given action from the commands topic.
// The default events topic is "events", the used topic can be configured during initialization.
// All received events are send over the returned go channel.
func (commander *Commander) NewEventConsumer(action string) (chan *Event, *Consumer) {
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
					match := false
					for _, header := range message.Headers {
						if header.Key == ActionHeader && string(header.Value) == action {
							match = true
						}
					}

					if !match {
						continue
					}

					event := Event{}
					event.Populate(message)
					sink <- &event
				}
			}
		}
	}()

	return sink, consumer
}

// NewCommandsConsumer starts consuming commands from the commands topic.
// The default commands topic is "commands", the used topic can be configured during initialization.
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

// NewCommandConsumer starts consuming commands with the given action from the commands topic.
// The default commands topic is "commands", the used topic can be configured during initialization.
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

// CommandHandle is a callback function mainly used to handle commands
type CommandHandle func(*Command)

// NewCommandHandle is a small wrapper around NewCommandConsumer that awaits till the given action is received.
// Once the action is received is the CommandHandle callback called.
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

// EventHandle is a callback function mainly used to handle events
type EventHandle func(*Event)

// NewEventHandle is a small wrapper around NewEventConsumer that awaits till the given event is received.
// Once the event is received is the EventHandle callback called.
func (commander *Commander) NewEventHandle(action string, callback EventHandle) func() {
	commands, consumer := commander.NewEventConsumer(action)

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
		TopicPartition: kafka.TopicPartition{Topic: &commander.CommandTopic, Partition: kafka.PartitionAny},
		Value:          command.Data,
	}

	log.Println("producing command with action:", command.Action)
	return commander.Produce(&message)
}

// SyncCommand produces a new command and waits for the resulting event.
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
			kafka.Header{
				Key:   AcknowledgedHeader,
				Value: []byte(strconv.FormatBool(event.Acknowledged)),
			},
		},
		Key:            event.ID.Bytes(),
		TopicPartition: kafka.TopicPartition{Topic: &commander.EventTopic},
		Value:          event.Data,
	}

	log.Println("producing event with action:", event.Action)
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
