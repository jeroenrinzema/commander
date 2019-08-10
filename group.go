package commander

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/jeroenrinzema/commander/metadata"
	"github.com/jeroenrinzema/commander/middleware"
	"github.com/jeroenrinzema/commander/types"
	log "github.com/sirupsen/logrus"
)

// Custom error types
var (
	ErrNoTopic  = errors.New("no topic found")
	ErrNoAction = errors.New("no action defined")
)

const (
	// DefaultAttempts represents the default amount of retry attempts
	DefaultAttempts = 5
	// DefaultTimeout represents the default timeout when awaiting a "sync" command to complete
	DefaultTimeout = 5 * time.Second
)

// EventType represents a middleware event type
type EventType string

// Globally available event types
const (
	AfterActionConsumption   = EventType("AfterActionConsumption")
	BeforeActionConsumption  = EventType("BeforeActionConsumption")
	BeforeMessageConsumption = EventType("BeforeMessageConsumption")
	AfterMessageConsumed     = EventType("AfterMessageConsumed")
	BeforePublish            = EventType("BeforePublish")
	AfterPublish             = EventType("AfterPublish")
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
		logger:  log.New(),
	}

	if os.Getenv(DebugEnv) != "" {
		group.logger.SetLevel(log.DebugLevel)
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
	logger     *log.Logger
}

// Close represents a closing method
type Close func()

// Next indicates that the next message could be called
type Next func(error)

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
	group.logger.Debug("executing async command")

	err := group.ProduceCommand(command)
	if err != nil {
		return err
	}

	return nil
}

// SyncCommand creates a command message to the given group and awaits
// its responding event message. If no message is received within the set timeout period
// will a timeout be thrown.
func (group *Group) SyncCommand(command Command) (event Event, next Next, err error) {
	group.logger.Debug("executing sync command")

	messages, next, closer, err := group.NewConsumerWithDeadline(group.Timeout, EventMessage)
	if err != nil {
		return event, next, err
	}

	defer closer()

	err = group.AsyncCommand(command)
	if err != nil {
		return event, next, err
	}

	event, next, err = group.AwaitEOS(messages, next, types.ParentID(command.ID))
	return event, next, err
}

// AwaitEventWithAction awaits till the first event for the given parent id and action is consumed.
// If no events are returned within the given timeout period a error will be returned.
func (group *Group) AwaitEventWithAction(messages <-chan *types.Message, next Next, parent types.ParentID, action string) (event Event, _ Next, err error) {
	group.logger.Debug("awaiting action")

	if action == "" {
		return event, next, ErrNoAction
	}

	for {
		message := <-messages
		if message == nil {
			return event, next, ErrTimeout
		}

		if message.Action != action {
			next(nil)
			continue
		}

		id, has := metadata.ParentIDFromContext(message.Ctx)
		if !has || parent != id {
			next(nil)
			continue
		}

		event = NewEventFromMessage(message)
		break
	}

	return event, next, nil
}

// AwaitEvent awaits till the first event is consumed for the given parent id.
// If no events are returned within the given timeout period a error will be returned.
func (group *Group) AwaitEvent(messages <-chan *types.Message, next Next, parent types.ParentID) (event Event, _ Next, err error) {
	group.logger.Debug("awaiting event")

	for {
		message := <-messages
		if message == nil {
			return event, next, ErrTimeout
		}

		id, has := metadata.ParentIDFromContext(message.Ctx)
		if !has || parent != id {
			next(nil)
			continue
		}

		event = NewEventFromMessage(message)
		break
	}

	return event, next, nil
}

// AwaitEOS awaits till the final event stream message is emitted.
// If no events are returned within the given timeout period a error will be returned.
func (group *Group) AwaitEOS(messages <-chan *types.Message, next Next, parent types.ParentID) (event Event, _ Next, err error) {
	group.logger.Debug("awaiting EOS")

	for {
		message := <-messages
		if message == nil {
			return event, next, ErrTimeout
		}

		if !message.EOS {
			next(nil)
			continue
		}

		id, has := metadata.ParentIDFromContext(message.Ctx)
		if !has || parent != id {
			next(nil)
			continue
		}

		group.logger.Debug("EOS message reached")
		event = NewEventFromMessage(message)

		break
	}

	return event, next, nil
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
	if command.Key == nil {
		command.Key = types.Key([]byte(command.ID))
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
	if event.Key == nil {
		event.Key = types.Key([]byte(event.ID))
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
	group.Middleware.Emit(message.Ctx, BeforePublish, message)
	defer group.Middleware.Emit(message.Ctx, AfterPublish, message)

	err := message.Topic.Dialect.Producer().Publish(message)
	if err != nil {
		return err
	}

	return nil
}

// NewConsumer starts consuming events of topics from the same topic type.
// All received messages are published over the returned messages channel.
// All middleware subscriptions are called before consuming the message.
// Once a message is consumed should the next function be called to mark a message successfully consumed.
func (group *Group) NewConsumer(sort types.MessageType) (<-chan *types.Message, Next, Close, error) {
	group.logger.Debugf("new message consumer %d", sort)

	topics := group.FetchTopics(sort, ConsumeMode)
	if len(topics) == 0 {
		return make(<-chan *Message, 0), func(error) {}, func() {}, ErrNoTopic
	}

	// NOTE: support multiple topics for consumption?
	topic := topics[0]

	sink := make(chan *Message, 0)
	messages, next, err := topic.Dialect.Consumer().Subscribe(topics...)
	if err != nil {
		close(sink)

		return sink, func(error) {}, func() {}, err
	}

	mutex := sync.Mutex{}
	breaker := Breaker{}

	go func() {
		for message := range messages {
			if !breaker.Safe() {
				next(nil)
				return
			}

			group.logger.Debug("message consumer consumed message")

			mutex.Lock()
			group.Middleware.Emit(message.Ctx, BeforeMessageConsumption, message)

			sink <- message

			group.Middleware.Emit(message.Ctx, AfterMessageConsumed, message)
			mutex.Unlock()
		}
	}()

	closer := func() {
		mutex.Lock()
		defer mutex.Unlock()

		if !breaker.Safe() {
			return
		}

		breaker.Open()
		close(sink)

		go topic.Dialect.Consumer().Unsubscribe(messages)
	}

	return sink, next, closer, nil
}

// NewConsumerWithDeadline consumes events of the given message type for the given duration.
// The message channel is closed once the deadline is reached.
// Once a message is consumed should the next function be called to mark a successfull consumption.
// The consumer could be closed premature by calling the close method.
func (group *Group) NewConsumerWithDeadline(timeout time.Duration, t types.MessageType) (<-chan *types.Message, Next, Close, error) {
	group.logger.Debugf("new consumer with deadline: %s", timeout)

	messages, next, closer, err := group.NewConsumer(t)
	if err != nil {
		return nil, nil, nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	closing := func() {
		cancel()
		closer()
	}

	go func() {
		<-ctx.Done()
		closing()
	}()

	return messages, next, closing, nil
}

// HandleFunc awaits messages from the given MessageType and action.
// Once a message is received is the callback method called with the received command.
// The handle is closed once the consumer receives a close signal.
func (group *Group) HandleFunc(sort types.MessageType, action string, callback Handle) (Close, error) {
	messages, next, closing, err := group.NewConsumer(sort)
	if err != nil {
		return nil, err
	}

	go func() {
		for message := range messages {
			if message.Action != action {
				next(nil)
				continue
			}

			group.Middleware.Emit(message.Ctx, BeforeActionConsumption, message)

			var value interface{}

			switch sort {
			case EventMessage:
				value = NewEventFromMessage(message)
			case CommandMessage:
				value = NewCommandFromMessage(message)
			}

			writer := NewResponseWriter(group, value)
			callback(writer, value)

			// Check if the message is marked to be retried
			res := writer.(retry)
			err := res.ShouldRetry()
			next(err)

			group.Middleware.Emit(message.Ctx, AfterActionConsumption, message)
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
