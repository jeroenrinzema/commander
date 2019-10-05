package middleware

import (
	"sync"

	"github.com/jeroenrinzema/commander/internal/types"
)

// HandleFunc represents a middleware handle method
type HandleFunc func(types.Handle) types.Handle

// Controller middleware controller
type Controller interface {
	Middleware(HandleFunc)
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

// Run executes the given middleware in chronological order.
// A handle function is returned once all middleware is executed.
func Run(h types.Handle, m ...HandleFunc) types.Handle {
	if len(m) < 1 {
		return h
	}

	wrapped := h

	// loop in reverse to preserve middleware order
	for i := len(m) - 1; i >= 0; i-- {
		wrapped = m[i](wrapped)
	}

	return wrapped
}
