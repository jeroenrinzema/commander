package middleware

import (
	"context"
	"sync"
)

// EventType represents a middleware event type
type EventType string

// Available events
const (
	AfterActionConsumption   EventType = "AfterActionConsumption"
	BeforeActionConsumption  EventType = "BeforeActionConsumption"
	BeforeMessageConsumption EventType = "BeforeMessageConsumption"
	AfterMessageConsumed     EventType = "AfterMessageConsumed"
	BeforePublish            EventType = "BeforePublish"
	AfterPublish             EventType = "AfterPublish"
)

// Handle represents a middleware handle
type Handle func(event *Event) error

// Service represents a middleware service
type Service interface {
	// Controller represents a middleware controller to initialize the passed middleware
	Controller(subscribe Subscribe)
}

// Subscribe represents a
type Subscribe func(event EventType, handle Handle)

// Collection holds a collection of middleware event subscriptions
type Collection struct {
	subscriptions []Handle
	mutex         sync.RWMutex
}

// Event represents a middle ware event message.
// The struct contains the given context and value
type Event struct {
	Value interface{}
	Ctx   context.Context
}

// NewClient constructs a new middleware client
func NewClient() *Client {
	client := &Client{
		events: make(map[EventType]*Collection),
	}

	return client
}

// Client handles all middleware event subscriptions.
// If a event is emitted are the subscribed middleware methods called and awaited.
type Client struct {
	events map[EventType]*Collection
	mutex  sync.RWMutex
}

// Emit calls all the subscribed middleware handles on the given event type.
// Each handle is called and awaited in order for it to manipulate or process a response.
// If a handle returns a error message is the manipulated message ignored.
func (client *Client) Emit(event EventType, message *Event) {
	client.mutex.RLock()
	defer client.mutex.RUnlock()

	if client.events[event] == nil {
		return
	}

	client.events[event].mutex.RLock()
	defer client.events[event].mutex.RUnlock()

	for _, handle := range client.events[event].subscriptions {
		err := handle(message)
		if err != nil {
			continue
		}
	}
}

// Subscribe creates a new middleware subscription for the given event type.
func (client *Client) Subscribe(event EventType, handle Handle) {
	if client.events[event] == nil {
		client.mutex.Lock()
		client.events[event] = &Collection{subscriptions: []Handle{}}
		client.mutex.Unlock()
	}

	client.events[event].mutex.Lock()
	defer client.events[event].mutex.Unlock()

	client.events[event].subscriptions = append(client.events[event].subscriptions, handle)
}

// Use calles the given middleware controller to initialize the middleware
func (client *Client) Use(service Service) {
	service.Controller(client.Subscribe)
}
