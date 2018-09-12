package commander

import (
	"context"
	"errors"
	"time"

	"github.com/Shopify/sarama"
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

// Topic is a small struct that contains information about the topic.
type Topic struct {
	Name    string
	Consume bool
	Timeout time.Duration
}

// Group contains information about a commander group.
// A commander group could contain a events and commands topic where
// commands and events could be consumed and produced to.
type Group struct {
	*Commander
	Name          string
	EventsTopic   Topic
	CommandsTopic Topic
}

// AsyncCommand creates a command message to the given group command topic
// and does not await for the responding event.
//
// If no command key is set will the command id be used. A command key is used
// to write a command to the right kafka partition therefor to guarantee the order
// of the kafka messages is it important to define a "dataset" key.
func (group *Group) AsyncCommand(command *Command) error {
	if command.Key == uuid.Nil {
		command.Key = command.ID
	}

	return group.ProduceCommand(command)
}

// SyncCommand creates a command message to the given group and awaits
// its responding message. If no message is received within the set timeout period
// will a timeout be thrown.
func (group *Group) SyncCommand(command *Command) (*Event, error) {
	err := group.AsyncCommand(command)
	if err != nil {
		return nil, err
	}

	events, closing := group.NewEventsConsumer()
	ctx, cancel := context.WithTimeout(context.Background(), group.CommandsTopic.Timeout)

	defer closing()
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

// NewEvent creates a new event for the given group.
func (group *Group) NewEvent() {
}

// ProduceCommand constructs and produces a command kafka message to the group command topic.
// A error is returned if anything went wrong in the process.
func (group *Group) ProduceCommand(command *Command) error {
	message := sarama.ProducerMessage{
		Headers: []sarama.RecordHeader{
			sarama.RecordHeader{
				Key:   []byte(ActionHeader),
				Value: []byte(command.Action),
			},
			sarama.RecordHeader{
				Key:   []byte(IDHeader),
				Value: []byte(command.ID.String()),
			},
		},
		Key:   sarama.StringEncoder(command.Key.String()),
		Value: sarama.ByteEncoder(command.Data),
		Topic: group.CommandsTopic.Name,
	}

	return group.Produce(&message)
}

// ProduceEvent produces a event kafka message to the group events topic.
// A error is returned if anything went wrong in the process.
func (group *Group) ProduceEvent(event *Event) error {
	return nil
}

// NewEventsConsumer starts consuming events of the given action and the given versions.
// The events topic used is set during initialization of the group.
// Two arguments are returned, a events channel and a method to unsubscribe the consumer.
// All received events are published over the returned events go channel.
func (group *Group) NewEventsConsumer() (chan *Event, func()) {

}

// NewCommandsConsumer starts consuming commands of the given action.
// The commands topic used is set during initialization of the group.
// Two arguments are returned, a events channel and a method to unsubscribe the consumer.
// All received events are published over the returned events go channel.
func (group *Group) NewCommandsConsumer(action string) (chan *Command, func()) {

}

// EventHandle is a callback function used to handle/process events
type EventHandle func(*Event)

// NewEventsHandle is a small wrapper around NewEventsConsumer but calls the given callback method instead.
// Once a event of the given action is received is the EventHandle callback called.
// The handle is closed once the consumer receives a close signal.
func (group *Group) NewEventsHandle(action string, versions []int, callback EventHandle) func() {
}

// CommandHandle is a callback function used to handle/process commands
type CommandHandle func(*Event)

// NewCommandsHandle is a small wrapper around NewCommandsConsumer but calls the given callback method instead.
// Once a event of the given action is received is the EventHandle callback called.
// The handle is closed once the consumer receives a close signal.
func (group *Group) NewCommandsHandle(action string, callback CommandHandle) func() {
}
