package middleware

import (
	"sync"

	"github.com/jeroenrinzema/commander/internal/types"
)

// HandlerFunc is called once a message is received.
// The next middleware is not called untill next() is called.
type HandlerFunc func(writer types.Writer, message *types.Message) error

// Controller middleware controller
type Controller interface {
	Middleware(HandlerFunc)
}

// Client represents a middleware client
type Client interface {
	Use(Controller)
}

// Use exposed usage interface
type Use interface {
	Use(Controller)
}

// NewClient constructs a new middleware client
func NewClient() Client {
	client := &client{
		middlewares: []Controller{},
	}

	return client
}

type client struct {
	middlewares []Controller
	mutex       sync.RWMutex
}

// Use calles the given middleware controller to initialize the middleware
func (client *client) Use(controller Controller) {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	client.middlewares = append(client.middlewares, controller)
}

func (client *client) run(writer types.Writer, message *types.Message) func() {
	return func() {

	}
}
