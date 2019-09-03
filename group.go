package commander

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/jeroenrinzema/commander/circuit"
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
func NewGroup(definitions ...types.GroupOption) *Group {
	options := types.NewGroupOptions(definitions)

	group := &Group{
		Timeout: options.Timeout,
		Retries: options.Retries,
		Topics:  options.Topics,
		Codec:   options.Codec,
		logger:  log.New(),
	}

	// NOTE: possible creation of a "universal" logger interface that could easily be implemented.
	// Log levels should be defined/set outside of commander
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
	Topics     []types.Topic
	Codec      types.Codec
	Retries    int8
	logger     *log.Logger
}

// Close represents a closing method
type Close = types.Close

// Next indicates that the next message could be called
type Next = types.Next

// Handle message handle message, writer implementation
type Handle = types.Handle

// Handler interface handle wrapper
type Handler = types.Handler

// AsyncCommand produces a message to the given group command topic
// and does not await for the responding event. If no command key is set will the command id be used.
func (group *Group) AsyncCommand(message *Message) error {
	group.logger.Debug("executing async command")

	err := group.ProduceCommand(message)
	if err != nil {
		return err
	}

	return nil
}

// SyncCommand produces a message to the given group command topic and awaits
// its responding event message. If no message is received within the set timeout period
// will a timeout be thrown.
func (group *Group) SyncCommand(message *Message) (event *Message, err error) {
	group.logger.Debug("executing sync command")

	messages, closer, err := group.NewConsumerWithDeadline(group.Timeout, EventMessage)
	if err != nil {
		return event, err
	}

	defer closer()

	err = group.AsyncCommand(message)
	if err != nil {
		return event, err
	}

	event, err = group.AwaitEOS(messages, types.ParentID(message.ID))
	return event, err
}

// AwaitEventWithAction awaits till the first event for the given parent id and action is consumed.
// If no events are returned within the given timeout period a error will be returned.
func (group *Group) AwaitEventWithAction(messages <-chan *types.Message, parent types.ParentID, action string) (message *Message, err error) {
	group.logger.Debug("awaiting action")

	if action == "" {
		return message, ErrNoAction
	}

	for {
		message = <-messages
		if message == nil {
			return nil, ErrTimeout
		}

		if message.Action != action {
			message.Next()
			continue
		}

		id, has := types.ParentIDFromContext(message.Ctx)
		if !has || parent != id {
			message.Next()
			continue
		}

		break
	}

	return message, nil
}

// AwaitMessage awaits till the first message is consumed for the given parent id.
// If no events are returned within the given timeout period a error will be returned.
func (group *Group) AwaitMessage(messages <-chan *types.Message, parent types.ParentID) (message *Message, err error) {
	group.logger.Debug("awaiting message")

	for {
		message = <-messages
		if message == nil {
			return nil, ErrTimeout
		}

		id, has := types.ParentIDFromContext(message.Ctx)
		if !has || parent != id {
			message.Next()
			continue
		}

		break
	}

	return message, nil
}

// AwaitEOS awaits till the final event stream message is emitted.
// If no events are returned within the given timeout period a error will be returned.
func (group *Group) AwaitEOS(messages <-chan *types.Message, parent types.ParentID) (message *Message, err error) {
	group.logger.Debug("awaiting EOS")

	for {
		message = <-messages
		if message == nil {
			return message, ErrTimeout
		}

		if !message.EOS {
			message.Next()
			continue
		}

		id, has := types.ParentIDFromContext(message.Ctx)
		if !has || parent != id {
			message.Next()
			continue
		}

		group.logger.Debug("EOS message reached")
		break
	}

	return message, nil
}

// FetchTopics fetches the available topics for the given mode and the given type
func (group *Group) FetchTopics(t types.MessageType, m types.TopicMode) []types.Topic {
	topics := []Topic{}

	for _, topic := range group.Topics {
		if topic.Type() != t {
			continue
		}

		if !topic.HasMode(m) {
			continue
		}

		topics = append(topics, topic)
	}

	return topics
}

// ProduceCommand produce a message to the given group command topic.
// A error is returned if anything went wrong in the process. If no command key is set will the command id be used.
func (group *Group) ProduceCommand(message *Message) error {
	if message.Key == nil {
		message.Key = types.Key([]byte(message.ID))
	}

	topics := group.FetchTopics(CommandMessage, ProduceMode)
	if len(topics) == 0 {
		return ErrNoTopic
	}

	// NOTE: Support for multiple produce topics?
	// Possible, but error handling has to be easily handled when errors occures at one of the topics in the process of publishing
	topic := topics[0]
	message.Topic = topic

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
func (group *Group) ProduceEvent(message *Message) error {
	if message.Key == nil {
		message.Key = types.Key([]byte(message.ID))
	}

	topics := group.FetchTopics(EventMessage, ProduceMode)
	if len(topics) == 0 {
		return ErrNoTopic
	}

	// NOTE: Support for multiple produce topics?
	// Possible, but error handling has to be easily handled when errors occures at one of the topics in the process of publishing
	topic := topics[0]
	message.Topic = topic

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

	err := message.Topic.Dialect().Producer().Publish(message)
	if err != nil {
		return err
	}

	return nil
}

// NewConsumer starts consuming events of topics from the same topic type.
// All received messages are published over the returned messages channel.
// All middleware subscriptions are called before consuming the message.
// Once a message is consumed should the next function be called to mark a message successfully consumed.
func (group *Group) NewConsumer(sort types.MessageType) (<-chan *types.Message, Close, error) {
	group.logger.Debugf("new message consumer: %d", sort)

	topics := group.FetchTopics(sort, ConsumeMode)
	if len(topics) == 0 {
		return make(<-chan *Message, 0), func() {}, ErrNoTopic
	}

	// NOTE: support multiple topics for consumption?
	topic := topics[0]

	sink := make(chan *Message, 0)
	messages, err := topic.Dialect().Consumer().Subscribe(topics...)
	if err != nil {
		close(sink)

		return sink, func() {}, err
	}

	mutex := sync.Mutex{}
	breaker := circuit.Breaker{}

	go func() {
		for message := range messages {
			if !breaker.Safe() {
				message.Next()
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

		go topic.Dialect().Consumer().Unsubscribe(messages)
	}

	return sink, closer, nil
}

// NewConsumerWithDeadline consumes events of the given message type for the given duration.
// The message channel is closed once the deadline is reached.
// Once a message is consumed should the next function be called to mark a successfull consumption.
// The consumer could be closed premature by calling the close method.
func (group *Group) NewConsumerWithDeadline(timeout time.Duration, t types.MessageType) (<-chan *types.Message, Close, error) {
	group.logger.Debugf("new consumer with deadline: %s", timeout)

	messages, closer, err := group.NewConsumer(t)
	if err != nil {
		return nil, nil, err
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

	return messages, closing, nil
}

// Handle awaits messages from the given MessageType and action.
// Once a message is received is the callback method called with the received command.
// The handle is closed once the consumer receives a close signal.
func (group *Group) Handle(sort types.MessageType, action string, handler Handler) (Close, error) {
	return group.HandleFunc(sort, action, handler.Handle)
}

// HandleFunc awaits messages from the given MessageType and action.
// Once a message is received is the callback method called with the received command.
// The handle is closed once the consumer receives a close signal.
func (group *Group) HandleFunc(sort types.MessageType, action string, callback Handle) (Close, error) {
	return group.HandleContext(
		WithAction(action),
		WithMessageType(sort),
		WithCallback(callback),
		WithMessageSchema(group.Codec.Schema()),
	)
}

// HandleContext constructs a handle context based on the given definitions.
func (group *Group) HandleContext(definitions ...types.HandleOption) (Close, error) {
	options := types.NewHandleOptions(definitions)
	group.logger.Debugf("setting up new consumer handle: %d, %s", options.MessageType, options.Action)

	messages, closing, err := group.NewConsumer(options.MessageType)
	if err != nil {
		return nil, err
	}

	go func() {
		for message := range messages {
			if options.Action != "" && message.Action != options.Action {
				message.Next()
				continue
			}

			// FIXME: options.Schema is now a shared value
			group.Codec.Unmarshal(message.Data, &options.Schema)
			message.NewSchema(options.Schema)

			group.Middleware.Emit(message.Ctx, BeforeActionConsumption, message)

			writer := NewWriter(group, message)
			options.Callback(message, writer)

			message.Next()

			group.Middleware.Emit(message.Ctx, AfterActionConsumption, message)
		}
	}()

	return closing, nil
}
