package middleware

import (
	"sync"

	"github.com/jeroenrinzema/commander/internal/types"
)

// BeforeConsumeHandlerFunc represents the function method called and returned by a middleware client
type BeforeConsumeHandlerFunc = types.HandlerFunc

// BeforeProduceHandlerFunc represents the function method called and returned by a middleware client
type BeforeProduceHandlerFunc = func(*types.Message)

// ConsumeController middleware controller
type ConsumeController interface {
	BeforeConsume(types.HandlerFunc) types.HandlerFunc
}

// ProduceController middleware controller
type ProduceController interface {
	BeforeProduce(*types.Message) *types.Message
}

// Client middleware interface
type Client interface {
	WrapBeforeConsume(BeforeConsumeHandlerFunc) BeforeConsumeHandlerFunc
	WrapBeforeProduce(BeforeProduceHandlerFunc) BeforeProduceHandlerFunc
}

// Use exposed usage interface
type Use interface {
	Use(interface{})
}

// NewClient constructs a new middleware client
func NewClient() Use {
	client := &client{
		consume: []ConsumeController{},
		produce: []ProduceController{},
	}

	return client
}

type client struct {
	consume []ConsumeController
	produce []ProduceController

	mutex sync.RWMutex
}

// Use calles the given middleware controller to initialize the middleware
func (client *client) Use(value interface{}) {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	if controller, ok := value.(ConsumeController); ok {
		client.consume = append(client.consume, controller)
	}

	if controller, ok := value.(ProduceController); ok {
		client.produce = append(client.produce, controller)
	}
}

// WrapBeforeConsume executes defined consume middleware in chronological order.
// A handle executable handler is returned once all middleware is wrapped.
func (client *client) WrapBeforeConsume(h types.HandlerFunc) types.HandlerFunc {
	if len(client.consume) < 1 {
		return h
	}

	wrapped := h

	// loop in reverse to preserve middleware order
	for i := len(client.consume) - 1; i >= 0; i-- {
		wrapped = client.consume[i].BeforeConsume(wrapped)
	}

	return wrapped
}

// WrapBeforeProduce executes defined produce middleware in chronological order.
// A handle executable handler is returned once all middleware is wrapped.
func (client *client) WrapBeforeProduce(m *types.Message) {
	// loop in reverse to preserve middleware order
	for i := len(client.produce) - 1; i >= 0; i-- {
		client.produce[i].BeforeProduce(m)
	}
}
