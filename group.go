package commander

import (
	"context"
	"errors"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander/middleware"
	"github.com/jeroenrinzema/commander/types"
)

// Custom error types
var (
	ErrNoTopic = errors.New("no topic found")
)

const (
	// DefaultAttempts represents the default amount of retry attempts
	DefaultAttempts = 5
	// DefaultTimeout represents the default timeout when awaiting a "sync" command to complete
	DefaultTimeout = 5 * time.Second
)

// NewGroup initializes a new commander group.
func NewGroup(t ...Topic) *Group {
	topics := make(map[types.TopicMode][]types.Topic)
	for _, topic := range t {
		if topic.HasMode(ConsumeMode) {
			topics[ConsumeMode] = append(topics[ConsumeMode], topic)
		}

		if topic.HasMode(ProduceMode) {
			topics[ProduceMode] = append(topics[ProduceMode], topic)
		}
	}

	group := &Group{
		Timeout: DefaultTimeout,
		Retries: DefaultAttempts,
		Topics:  topics,
	}

	return group
}

// Group contains information about a commander group.
// A commander group could contain a events and commands topic where
// commands and events could be consumed and produced to. The amount of retries
// attempted before a error is thrown could also be defined in a group.
type Group struct {
	Middleware *middleware.Client
	Timeout    time.Duration
	Topics     map[types.TopicMode][]types.Topic
	Retries    int
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
	sink, marked, erro := group.AwaitEvent(group.Timeout, command.ID, "")
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
func (group *Group) AwaitEvent(timeout time.Duration, parent uuid.UUID, action string) (<-chan Event, chan<- error, <-chan error) {
	Logger.Println("Awaiting child event")

	var match bool
	sink := make(chan Event, 1)
	erro := make(chan error, 1)

	messages, marked, closing, err := group.NewConsumer(EventMessage)
	if err != nil {
		closing()
		erro <- err
		return sink, marked, erro
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

	await:
		for {
			select {
			case <-ctx.Done():
				erro <- ErrTimeout
			case message := <-messages:
				if match {
					marked <- nil
					continue await
				}

				if action != "" && message.Headers[ActionHeader] != action {
					marked <- nil
					continue await
				}

				event := Event{}
				event.Populate(message)

				if parent != uuid.Nil && event.Parent != parent {
					marked <- nil
					continue await
				}

				go func() {
					match = true
					go closing()
					sink <- event
				}()
			}
		}
	}()

	return sink, marked, erro
}

// FetchTopics fetches the available topics for the given mode and the given type
func (group *Group) FetchTopics(t types.MessageType, m types.TopicMode) []types.Topic {
	topics := []Topic{}

	for _, topic := range group.Topics[m] {
		if topic.Type != t {
			continue
		}

		topics = append(topics, topic)
	}

	return topics
}

// ProduceCommand constructs and produces a command message to the set command topic.
// A error is returned if anything went wrong in the process. If no command key is set will the command id be used.
func (group *Group) ProduceCommand(command Command) error {
	Logger.Println("Producing command")

	if command.Key == uuid.Nil {
		command.Key = command.ID
	}

	topics := group.FetchTopics(CommandMessage, ProduceMode)
	if len(topics) == 0 {
		return ErrNoTopic
	}

	// NOTE: Support for multiple produce topics?
	topic := topics[0]

	message := command.Message(topic)
	amount := group.Retries
	if amount == 0 {
		amount = DefaultAttempts
	}

	retry := Retry{
		Amount: amount,
	}

	err := retry.Attempt(func() error {
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

	if event.Key == uuid.Nil {
		event.Key = event.ID
	}

	topics := group.FetchTopics(EventMessage, ProduceMode)
	if len(topics) == 0 {
		return ErrNoTopic
	}

	// NOTE: Support for multiple produce topics?
	topic := topics[0]

	message := event.Message(topic)
	amount := group.Retries
	if amount == 0 {
		amount = DefaultAttempts
	}

	retry := Retry{
		Amount: amount,
	}

	err := retry.Attempt(func() error {
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
	group.Middleware.Emit(middleware.BeforePublish, &middleware.Event{
		Value: message,
		Ctx:   message.Ctx,
	})

	defer group.Middleware.Emit(middleware.AfterPublish, &middleware.Event{
		Value: message,
		Ctx:   message.Ctx,
	})

	err := message.Topic.Dialect.Producer().Publish(message)
	if err != nil {
		return err
	}

	return nil
}

// NewConsumer starts consuming events of topics from the same topic type.
// All received messages are published over the returned messages channel.
// All middleware subscriptions are called before consuming the message.
func (group *Group) NewConsumer(sort types.MessageType) (<-chan *types.Message, chan<- error, Close, error) {
	Logger.Println("New topic consumer:", sort)

	topics := group.FetchTopics(sort, ConsumeMode)
	if len(topics) == 0 {
		return make(<-chan *Message, 0), make(chan<- error, 0), func() {}, ErrNoTopic
	}

	// NOTE: support multiple topics for consumption?
	topic := topics[0]

	sink := make(chan *Message, 1)
	called := make(chan error, 1)

	messages, marked, err := topic.Dialect.Consumer().Subscribe(topics...)
	if err != nil {
		return sink, called, func() {}, err
	}

	go func(messages <-chan *Message) {
		for message := range messages {
			group.Middleware.Emit(middleware.BeforeMessageConsumption, &middleware.Event{
				Value: message,
				Ctx:   message.Ctx,
			})

			sink <- message
			marked <- <-called // Await called and pipe into marked

			group.Middleware.Emit(middleware.AfterMessageConsumed, &middleware.Event{
				Value: message,
				Ctx:   message.Ctx,
			})
		}
	}(messages)

	return sink, called, func() { topic.Dialect.Consumer().Unsubscribe(messages) }, nil
}

// HandleFunc awaits messages from the given MessageType and action.
// Once a message is received is the callback method called with the received command.
// The handle is closed once the consumer receives a close signal.
func (group *Group) HandleFunc(sort types.MessageType, action string, callback Handle) (Close, error) {
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

			group.Middleware.Emit(middleware.BeforeActionConsumption, &middleware.Event{
				Value: message,
				Ctx:   message.Ctx,
			})

			switch sort {
			case EventMessage:
				event := Event{
					Ctx: message.Ctx,
				}
				event.Populate(message)

				value = event
			case CommandMessage:
				command := Command{
					Ctx: message.Ctx,
				}
				command.Populate(message)

				value = command
			}

			writer := NewResponseWriter(group, value)
			callback(writer, value)

			// Check if the message is marked to be retried
			err := writer.ShouldRetry()
			marked <- err

			group.Middleware.Emit(middleware.AfterActionConsumption, &middleware.Event{
				Value: message,
				Ctx:   message.Ctx,
			})
		}
	}()

	return closing, nil
}

// Handle awaits messages from the given MessageType and action.
// Once a message is received is the callback method called with the received command.
// The handle is closed once the consumer receives a close signal.
func (group *Group) Handle(sort types.MessageType, action string, handler Handler) (Close, error) {
	return group.HandleFunc(sort, action, handler.Process)
}
