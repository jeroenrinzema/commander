package commander

import (
	"context"
	"errors"
	"strconv"
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
)

// Group contains information about a commander group.
// A commander group could contain a events and commands topic where
// commands and events could be consumed and produced to.
type Group struct {
	Client
	Timeout      time.Duration
	EventTopic   Topic
	CommandTopic Topic
}

// AsyncCommand creates a command message to the given group command topic
// and does not await for the responding event.
//
// If no command key is set will the command id be used. A command key is used
// to write a command to the right kafka partition therefor to guarantee the order
// of the kafka messages is it important to define a "dataset" key.
func (group *Group) AsyncCommand(command *Command) error {
	err := group.ProduceCommand(command)
	if err != nil {
		return err
	}

	return nil
}

// NewEvent creates a new acknowledged event.
func (group *Group) NewEvent(action string, version int, parent uuid.UUID, key uuid.UUID, data []byte) *Event {
	id := uuid.NewV4()
	event := &Event{
		Parent:       parent,
		ID:           id,
		Action:       action,
		Data:         data,
		Key:          key,
		Acknowledged: true,
		Version:      version,
	}

	return event
}

// NewCommand creates a new command.
func (group *Group) NewCommand(action string, key uuid.UUID, data []byte) *Command {
	id := uuid.NewV4()
	command := &Command{
		Key:    key,
		ID:     id,
		Action: action,
		Data:   data,
	}

	return command
}

// SyncEvent creates a new event message to the given group.
// If a error occured while writing the event the the events topic(s)
func (group *Group) SyncEvent(event *Event) error {
	err := group.ProduceEvent(event)
	if err != nil {
		return err
	}

	return nil
}

// SyncCommand creates a command message to the given group and awaits
// its responding event message. If no message is received within the set timeout period
// will a timeout be thrown.
func (group *Group) SyncCommand(command *Command) (*Event, error) {
	var err error
	err = group.AsyncCommand(command)

	if err != nil {
		return nil, err
	}

	var event *Event
	event, err = group.AwaitEvent(group.Timeout, command.ID)

	if err != nil {
		return nil, err
	}

	return event, nil
}

// AwaitEvent awaits till the expected events are created with the given parent id.
// The returend events are buffered in the sink channel.
//
// If not the expected events are returned within the given timeout period
// will a error be returned. The timeout channel is closed when all
// expected events are received or after a timeout is thrown.
func (group *Group) AwaitEvent(timeout time.Duration, parent uuid.UUID) (*Event, error) {
	events, closing := group.NewEventConsumer()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	defer cancel()
	defer closing()

	for {
		select {
		case event := <-events:
			if event.Parent != parent {
				continue
			}

			return event, nil
		case <-ctx.Done():
			return nil, errors.New("timeout reached")
		}
	}
}

// ProduceCommand constructs and produces a command kafka message to the set command topic.
// A error is returned if anything went wrong in the process.
//
// If no command key is set will the command id be used. A command key is used
// to write a command to the right kafka partition therefor to guarantee the order
// of the kafka messages is it important to define a "dataset" key.
func (group *Group) ProduceCommand(command *Command) error {
	if command.Key == uuid.Nil {
		command.Key = command.ID
	}

	message := kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(command.Action),
			},
			kafka.Header{
				Key:   IDHeader,
				Value: []byte(command.ID.String()),
			},
		},
		Key:   []byte(command.Key.String()),
		Value: command.Data,
		TopicPartition: kafka.TopicPartition{
			Topic: &group.CommandTopic.Name,
		},
	}

	return group.Produce(&message)
}

// ProduceEvent produces a event kafka message to the set event topic.
// A error is returned if anything went wrong in the process.
func (group *Group) ProduceEvent(event *Event) error {
	if event.Key == uuid.Nil {
		event.Key = event.ID
	}

	message := kafka.Message{
		Headers: []kafka.Header{
			kafka.Header{
				Key:   ActionHeader,
				Value: []byte(event.Action),
			},
			kafka.Header{
				Key:   ParentHeader,
				Value: []byte(event.Parent.String()),
			},
			kafka.Header{
				Key:   IDHeader,
				Value: []byte(event.ID.String()),
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
		Key:   []byte(event.Key.String()),
		Value: event.Data,
		TopicPartition: kafka.TopicPartition{
			Topic: &group.EventTopic.Name,
		},
	}

	return group.Produce(&message)
}

// NewEventConsumer starts consuming events of the given action and the given versions.
// The events topic used is set during initialization of the group.
// Two arguments are returned, a events channel and a method to unsubscribe the consumer.
// All received events are published over the returned events go channel.
func (group *Group) NewEventConsumer() (chan *Event, func()) {
	sink := make(chan *Event, 1)
	messages, closing := group.Client.Consumer().Subscribe(group.EventTopic)

	go func() {
		for message := range messages {
			event := Event{}
			event.Populate(message)
			// TODO: handle the event populate error

			sink <- &event
		}
	}()

	return sink, closing
}

// NewCommandConsumer starts consuming commands of the given action.
// The commands topic used is set during initialization of the group.
// Two arguments are returned, a events channel and a method to unsubscribe the consumer.
// All received events are published over the returned events go channel.
func (group *Group) NewCommandConsumer() (chan *Command, func()) {
	sink := make(chan *Command, 1)
	messages, closing := group.Client.Consumer().Subscribe(group.CommandTopic)

	go func() {
		for message := range messages {
			command := Command{}
			command.Populate(message)
			sink <- &command
		}
	}()

	return sink, closing
}

// EventHandle is a callback function used to handle/process events
type EventHandle func(*Event)

// NewEventHandle is a small wrapper around NewEventConsumer but calls the given callback method instead.
// Once a event of the given action is received is the EventHandle callback called.
// The handle is closed once the consumer receives a close signal.
func (group *Group) NewEventHandle(action string, versions []int, callback EventHandle) func() {
	events, closing := group.NewEventConsumer()

	go func() {
		for event := range events {
			if event.Action != action {
				continue
			}

			for _, version := range versions {
				if version != event.Version {
					continue
				}

				callback(event)
				break
			}
		}
	}()

	return closing
}

// CommandHandle is a callback function used to handle/process commands
type CommandHandle func(*Command) *Event

// NewCommandHandle is a small wrapper around NewCommandConsumer but calls the given callback method instead.
// Once a event of the given action is received is the EventHandle callback called.
// The handle is closed once the consumer receives a close signal.
func (group *Group) NewCommandHandle(action string, callback CommandHandle) func() {
	commands, closing := group.NewCommandConsumer()

	go func() {
		for command := range commands {
			if command.Action != action {
				continue
			}

			event := callback(command)
			if event == nil {
				continue
			}

			group.SyncEvent(event)
		}
	}()

	return closing
}
