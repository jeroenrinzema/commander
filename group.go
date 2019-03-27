package commander

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
)

const (
	// DefaultAttempts represents the default amount of retry attempts
	DefaultAttempts = 5
)

// Group contains information about a commander group.
// A commander group could contain a events and commands topic where
// commands and events could be consumed and produced to. The amount of retries
// attempted before a error is thrown could also be defined in a group.
type Group struct {
	*Client
	Timeout time.Duration
	Topics  []Topic
	Retries int
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

// IsAttached checks if the given group is attached to a commander instance
func (group *Group) IsAttached() error {
	if group.Client == nil {
		topics := []string{}
		for _, topic := range group.Topics {
			topics = append(topics, topic.Name)
		}

		return fmt.Errorf("The commander group for the topics: %+v is not attached to a commander instance", topics)
	}

	return nil
}

// AsyncCommand creates a command message to the given group command topic
// and does not await for the responding event. If no command key is set will the command id be used.
func (group *Group) AsyncCommand(command Command) error {
	err := group.ProduceCommand(command)
	if err != nil {
		return err
	}

	return nil
}

// SyncCommand creates a command message to the given group and awaits
// its responding event message. If no message is received within the set timeout period
// will a timeout be thrown.
func (group *Group) SyncCommand(command Command) (Event, error) {
	sink, marked, erro := group.AwaitEvent(group.Timeout, command.ID)
	err := group.AsyncCommand(command)
	if err != nil {
		return Event{}, err
	}

	select {
	case event := <-sink:
		marked <- nil
		return event, nil
	case err := <-erro:
		return Event{}, err
	}
}

// AwaitEvent awaits till the expected events are created with the given parent id.
// The returend events are buffered in the sink channel.
//
// If not the expected events are returned within the given timeout period
// will a error be returned. The timeout channel is closed when all
// expected events are received or after a timeout is thrown.
func (group *Group) AwaitEvent(timeout time.Duration, parent uuid.UUID) (<-chan Event, chan<- error, <-chan error) {
	Logger.Println("Awaiting child event")

	sink := make(chan Event, 1)
	erro := make(chan error, 1)

	messages, marked, closing, err := group.NewConsumer(EventTopic)
	if err != nil {
		closing()
		erro <- err
		return sink, marked, erro
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer closing()
		defer cancel()

	await:
		for {
			select {
			case <-ctx.Done():
				erro <- ErrTimeout
				break await
			case message := <-messages:
				event := Event{}
				event.Populate(message)

				if event.Parent != parent {
					marked <- nil
					continue await
				}

				sink <- event
				break await
			}
		}
	}()

	return sink, marked, erro
}

// FetchConsumeTopic returns the active consume/produce topic of the given type
func (group *Group) FetchConsumeTopic(sType TopicType) (Topic, error) {
	for _, topic := range group.Topics {
		if topic.Type != sType {
			continue
		}

		if topic.Consume != true {
			continue
		}

		return topic, nil
	}

	return Topic{}, errors.New("No topic is found that is marked for consumption")
}

// FetchProduceTopic returns the active consume/produce topic of the given type
func (group *Group) FetchProduceTopic(sType TopicType) (Topic, error) {
	for _, topic := range group.Topics {
		if topic.Type != sType {
			continue
		}

		if topic.Produce != true {
			continue
		}

		return topic, nil
	}

	return Topic{}, errors.New("No topic is found that is marked for producing")
}

// ProduceCommand constructs and produces a command message to the set command topic.
// A error is returned if anything went wrong in the process. If no command key is set will the command id be used.
func (group *Group) ProduceCommand(command Command) error {
	Logger.Println("Producing command")

	err := group.IsAttached()
	if err != nil {
		panic(err)
	}

	if command.Key == uuid.Nil {
		command.Key = command.ID
	}

	topic, err := group.FetchProduceTopic(CommandTopic)
	if err != nil {
		return err
	}

	message := command.Message(topic)
	amount := group.Retries
	if amount == 0 {
		amount = DefaultAttempts
	}

	retry := Retry{
		Amount: amount,
	}

	err = retry.Attempt(func() error {
		return group.Publish(message)
	})

	if err != nil {
		return err
	}

	return nil
}

// ProduceEvent produces a event kafka message to the set event topic.
// A error is returned if anything went wrong in the process.
func (group *Group) ProduceEvent(event Event) error {
	Logger.Println("Producing event")

	err := group.IsAttached()
	if err != nil {
		panic(err)
	}

	if event.Key == uuid.Nil {
		event.Key = event.ID
	}

	topic, err := group.FetchProduceTopic(EventTopic)
	if err != nil {
		return err
	}

	message := event.Message(topic)
	amount := group.Retries
	if amount == 0 {
		amount = DefaultAttempts
	}

	retry := Retry{
		Amount: amount,
	}

	err = retry.Attempt(func() error {
		return group.Publish(message)
	})

	if err != nil {
		return err
	}

	return nil
}

// Publish publishes the given message to the group producer.
// All middleware subscriptions are called before publishing the message.
func (group *Group) Publish(message *Message) error {
	group.Middleware.Emit(BeforePublish, message)
	return group.Producer.Publish(message)
}

// NewConsumer starts consuming events of topics from the same topic type.
// All received messages are published over the returned messages channel.
// All middleware subscriptions are called before consuming the message.
func (group *Group) NewConsumer(sort TopicType) (<-chan *Message, chan<- error, Close, error) {
	Logger.Println("New topic consumer")

	err := group.IsAttached()
	if err != nil {
		panic(err)
	}

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

	if len(topics) == 0 {
		return make(<-chan *Message, 0), make(chan<- error, 0), func() {}, errors.New("no consumable topics are found for the topic type" + string(sort))
	}

	sink := make(chan *Message, 1)
	called := make(chan error, 1)

	messages, marked, err := group.Consumer.Subscribe(topics...)

	go func(messages <-chan *Message) {
		for message := range messages {
			group.Middleware.Emit(BeforeConsumption, message)
			sink <- message
			marked <- <-called // Await called and pipe into marked
			group.Middleware.Emit(AfterConsumed, message)
		}
	}(messages)

	return sink, called, func() { group.Consumer.Unsubscribe(messages) }, err
}

// HandleFunc awaits messages from the given TopicType and action.
// Once a message is received is the callback method called with the received command.
// The handle is closed once the consumer receives a close signal.
func (group *Group) HandleFunc(sort TopicType, action string, callback Handle) (Close, error) {
	messages, marked, closing, err := group.NewConsumer(sort)
	if err != nil {
		return nil, err
	}

	go func() {
		for message := range messages {
			var value interface{}

			a := string(message.Headers[ActionHeader])
			if a != action {
				marked <- nil
				continue
			}

			Logger.Println("Processing action:", a)

			switch sort {
			case EventTopic:
				event := Event{}
				event.Populate(message)

				value = event
			case CommandTopic:
				command := Command{}
				command.Populate(message)

				value = command
			}

			writer := NewResponseWriter(group, value)
			callback(writer, value)

			// Check if the message is marked to be retried
			err := writer.ShouldRetry()
			marked <- err
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
