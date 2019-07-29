package middleware

import (
	"context"
	"sync"
)

// Handle represents a middleware handle
type Handle func(ctx context.Context, message interface{})

// Subscribe represents a subscribe handler
type Subscribe func(event interface{}, handle Handle)

// Collection holds a collection of middleware event subscriptions
type Collection struct {
	subscriptions []Handle
	mutex         sync.Mutex
}

// NewClient constructs a new middleware client
func NewClient() *Client {
	client := &Client{
		events: make(map[interface{}]*Collection),
	}

	return client
}

// Client handles all middleware event subscriptions.
// If a event is emitted are the subscribed middleware methods called and awaited.
type Client struct {
	events map[interface{}]*Collection
	mutex  sync.Mutex
}

// Emit calls all the subscribed middleware handles on the given event type.
// Each handle is called and awaited in order for it to manipulate or process a response.
// If a handle returns a error message is the manipulated message ignored.
func (client *Client) Emit(ctx context.Context, event interface{}, message interface{}) {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	if client.events[event] == nil {
		return
	}

	client.events[event].mutex.Lock()
	defer client.events[event].mutex.Unlock()

	for _, handle := range client.events[event].subscriptions {
		handle(ctx, message)
	}
}

// Subscribe creates a new middleware subscription for the given event type.
func (client *Client) Subscribe(event interface{}, handle Handle) {
	if client.events[event] == nil {
		client.mutex.Lock()
		client.events[event] = &Collection{subscriptions: []Handle{}}
		client.mutex.Unlock()
	}

	client.events[event].mutex.Lock()
	defer client.events[event].mutex.Unlock()

	client.events[event].subscriptions = append(client.events[event].subscriptions, handle)
}

// // Use calles the given middleware controller to initialize the middleware
// func (client *Client) Use() {
// 	service.Controller(client.Subscribe)
// }
