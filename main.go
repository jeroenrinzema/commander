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
	Consumer      *kafka.Consumer
	Producer      *kafka.Producer
	Subscriptions []string
}

// Subscribe to the given topic. Two channels are returned when subscribing.
// The first channel get's feeded with kafka events that are happening on the subscribed topic.
// The second channel can be used to cancel the subscription.
func (commander *Commander) Subscribe(topic string) (chan kafka.Event, chan bool) {
	commander.Subscriptions = append(commander.Subscriptions, topic)
	commander.Consumer.SubscribeTopics(commander.Subscriptions, nil)

	messages := make(chan kafka.Event)
	done := make(chan bool)

	go func() {
		defer close(messages)
		defer close(done)

		for {
			select {
			case <-done:
				for index, subscription := range commander.Subscriptions {
					if subscription == topic {
						commander.Subscriptions = append(commander.Subscriptions[:index], commander.Subscriptions[index+1:]...)
					}
				}
				break
			case message := <-commander.Consumer.Events():
				switch event := message.(type) {
				case *kafka.Message:
					if *event.TopicPartition.Topic != topic {
						continue
					}

					messages <- message
				case kafka.PartitionEOF:
					if *event.Topic != topic {
						continue
					}

					messages <- message
				}
			}
		}
	}()

	return messages, done
}

// NewEventConsumer starts consuming the events from the events topic.
// The default events topic is "events", the used topic can be configured during initialization.
// All received messages are send over the "Event" channel.
func (commander *Commander) NewEventConsumer() (chan *Event, chan bool) {
	channel := make(chan *Event)
	events, close := commander.Subscribe(EventTopic)

	go func() {
		for {
			select {
			case <-close:
				break
			case e := <-events:
				switch message := e.(type) {
				case *kafka.Message:
					event := Event{}
					event.Populate(message)
					channel <- &event
				}
			}
		}
	}()

	return channel, close
}

// NewCommandConsumer starts consuming commands from the commands topic.
// The default commands topic is "commands", the used topic can be configured during initialization.
// All received messages are send over the "commands" channel.
func (commander *Commander) NewCommandConsumer() (chan *Command, chan bool) {
	commands := make(chan *Command)
	events, close := commander.Subscribe(CommandTopic)

	go func() {
		for {
			select {
			case <-close:
				break
			case e := <-events:
				switch message := e.(type) {
				case *kafka.Message:
					command := Command{}
					command.Populate(message)
					commands <- &command
				}
			}
		}
	}()

	return commands, close
}

// NewCommandHandle starts consuming commands with the given action from the commands topic.
// The default commands topic is "commands", the used topic can be configured during initialization.
// All received messages are send over the "commands" channel.
func (commander *Commander) NewCommandHandle(action string) (chan *Command, chan bool) {
	commands := make(chan *Command)
	events, close := commander.Subscribe(CommandTopic)

	go func() {
		for {
			select {
			case <-close:
				break
			case e := <-events:
				switch message := e.(type) {
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
					commands <- &command
				}
			}
		}
	}()

	return commands, close
}

// Produce a new kafka message
func (commander *Commander) Produce(message *kafka.Message) error {
	done := make(chan error)

	go func() {
		defer close(done)
		for e := range commander.Producer.Events() {
			switch event := e.(type) {
			case *kafka.Message:
				if event.TopicPartition.Error != nil {
					done <- event.TopicPartition.Error
					break
				}

				done <- nil
				break
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

	events, close := commander.NewEventConsumer()
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)

	defer func() { close <- true }()
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

// Close the commander consumer and producer
func (commander *Commander) Close() {
	commander.Producer.Close()
	commander.Consumer.Close()
}

// CloseOnSIGTERM close this commander instance once a SIGTERM signal is send
func (commander *Commander) CloseOnSIGTERM() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	commander.Close()
	os.Exit(0)
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
	conf.SetKey("go.application.rebalance.enable", true)

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
