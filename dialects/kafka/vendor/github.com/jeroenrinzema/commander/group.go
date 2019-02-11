package commander

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/gofrs/uuid"
)

const (
	// ParentHeader kafka message parent header
	ParentHeader = "parent"
	// ActionHeader kafka message action header
	ActionHeader = "action"
	// IDHeader kafka message id header
	IDHeader = "id"
	// StatusHeader kafka message status header
	StatusHeader = "status"
	// VersionHeader kafka message version header
	VersionHeader = "version"

	// DefaultAttempts represents the default ammount of retry attempts
	DefaultAttempts = 5
)

// Group contains information about a commander group.
// A commander group could contain a events and commands topic where
// commands and events could be consumed and produced to. The ammount of retries
// attempted before a error is thrown could also be defined in a group.
type Group struct {
	*Client
	Timeout time.Duration
	Topics  []Topic
	Retries int
	mutex   sync.Mutex
}

// Close represents a closing method
type Close func()

// Handle represents a message handle method
// The interface could contain a *Event or *Command struct
// regarding to the topic type that is being consumed.
type Handle func(ResponseWriter, interface{})

// Handler prodvides a interface to handle Messages
type Handler interface {
	Process(writer ResponseWriter, message interface{})
}

// AsyncCommand creates a command message to the given group command topic
// and does not await for the responding event. If no command key is set will the command id be used.
func (group *Group) AsyncCommand(command *Command) error {
	err := group.ProduceCommand(command)
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
	messages, closing, err := group.NewConsumer(EventTopic)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	defer cancel()
	defer closing()

	for {
		select {
		case message := <-messages:
			event := &Event{}
			event.Populate(message)

			if event.Parent != parent {
				continue
			}

			return event, nil
		case <-ctx.Done():
			return nil, errors.New("timeout reached")
		}
	}
}

// ProduceCommand constructs and produces a command message to the set command topic.
// A error is returned if anything went wrong in the process. If no command key is set will the command id be used.
func (group *Group) ProduceCommand(command *Command) error {
	if command.Key == uuid.Nil {
		command.Key = command.ID
	}

	for _, topic := range group.Topics {
		if topic.Type != CommandTopic {
			continue
		}

		if topic.Produce == false {
			continue
		}

		message := &Message{
			Headers: []Header{
				Header{
					Key:   ActionHeader,
					Value: []byte(command.Action),
				},
				Header{
					Key:   IDHeader,
					Value: []byte(command.ID.String()),
				},
			},
			Key:   []byte(command.Key.String()),
			Value: command.Data,
			Topic: topic,
		}

		ammount := group.Retries
		if ammount == 0 {
			ammount = DefaultAttempts
		}

		retry := Retry{
			Ammount: ammount,
		}

		err := retry.Attempt(func() error {
			return group.Producer.Publish(message)
		})

		if err != nil {
			return err
		}

		return nil
	}

	return errors.New("No command topic is found that could be produced to")
}

// ProduceEvent produces a event kafka message to the set event topic.
// A error is returned if anything went wrong in the process.
func (group *Group) ProduceEvent(event *Event) error {
	if event.Key == uuid.Nil {
		event.Key = event.ID
	}

	for _, topic := range group.Topics {
		if topic.Type != EventTopic {
			continue
		}

		if topic.Produce == false {
			continue
		}

		message := &Message{
			Headers: []Header{
				Header{
					Key:   ActionHeader,
					Value: []byte(event.Action),
				},
				Header{
					Key:   ParentHeader,
					Value: []byte(event.Parent.String()),
				},
				Header{
					Key:   IDHeader,
					Value: []byte(event.ID.String()),
				},
				Header{
					Key:   StatusHeader,
					Value: []byte(strconv.Itoa(event.Status)),
				},
				Header{
					Key:   VersionHeader,
					Value: []byte(strconv.Itoa(event.Version)),
				},
			},
			Key:   []byte(event.Key.String()),
			Value: event.Data,
			Topic: topic,
		}

		ammount := group.Retries
		if ammount == 0 {
			ammount = DefaultAttempts
		}

		retry := Retry{
			Ammount: ammount,
		}

		err := retry.Attempt(func() error {
			return group.Producer.Publish(message)
		})

		if err != nil {
			return err
		}

		return nil
	}

	return errors.New("No event topic is found that could be produced to")
}

// NewConsumer starts consuming events of topics from the same topic type.
// All received messages are published over the returned messages channel.
func (group *Group) NewConsumer(sort TopicType) (<-chan *Message, Close, error) {
	group.mutex.Lock()
	defer group.mutex.Unlock()

	topics := []Topic{}
	for _, topic := range group.Topics {
		if topic.Type != sort {
			continue
		}

		if topic.Consume == false {
			continue
		}

		topics = append(topics, topic)
	}

	messages, err := group.Consumer.Subscribe(topics...)
	return messages, func() { group.Consumer.Unsubscribe(messages) }, err
}

// HandleFunc awaits messages from the given TopicType and action.
// Once a message is received is the callback method called with the received command.
// The handle is closed once the consumer receives a close signal.
func (group *Group) HandleFunc(sort TopicType, action string, callback Handle) (Close, error) {
	messages, closing, err := group.NewConsumer(sort)
	if err != nil {
		return nil, err
	}

	go func() {
		for message := range messages {
			var value interface{}

			switch sort {
			case EventTopic:
				event := &Event{}
				event.Populate(message)

				value = event
			case CommandTopic:
				command := &Command{}
				command.Populate(message)

				value = command
			}

			writer := NewResponseWriter(group, value)
			callback(writer, value)
		}
	}()

	return closing, nil
}

// Handle awaits messages from the given TopicType and action.
// Once a message is received is the callback method called with the received command.
// The handle is closed once the consumer receives a close signal.
func (group *Group) Handle(sort TopicType, action string, handler Handler) (Close, error) {
	return group.HandleFunc(sort, action, handler.Process)
}
