package commander

import (
	"context"
	"sync"
)

// EventType represents a middleware event type
type EventType string

// Available events
const (
	BeforeConsumption EventType = "BeforeConsumption"
	AfterConsumed     EventType = "AfterConsumed"
	BeforePublish     EventType = "BeforePublish"
	AfterPublish      EventType = "AfterPublish"
)

// MiddlewareHandle represents a middleware handle
type MiddlewareHandle func(event *MiddlewareEvent) error

// MiddlewareController represents a middleware controller to initialize the passed middleware
type MiddlewareController func(subscribe MiddlewareSubscribe)

// MiddlewareSubscribe represents a
type MiddlewareSubscribe func(event EventType, handle MiddlewareHandle)

// MiddlewareSubscriptions holds a group of middleware subscriptions
type MiddlewareSubscriptions struct {
	subscriptions []MiddlewareHandle
	mutex         sync.RWMutex
}

// MiddlewareEvent represents a middle ware event message.
// The struct contains the given context and value
type MiddlewareEvent struct {
	Value interface{}
	Ctx   context.Context
}

// Middleware handles all middleware event subscriptions.
// If a event is emitted are the subscribed middleware methods called and awaited.
type Middleware struct {
	events map[EventType]*MiddlewareSubscriptions
	mutex  sync.RWMutex
}

// Emit calls all the subscribed middleware handles on the given event type.
// Each handle is called and awaited in order for it to manipulate or process a response.
// If a handle returns a error message is the manipulated message ignored.
func (middleware *Middleware) Emit(event EventType, message *MiddlewareEvent) {
	middleware.mutex.RLock()
	defer middleware.mutex.RUnlock()

	if middleware.events[event] == nil {
		return
	}

	middleware.events[event].mutex.RLock()
	defer middleware.events[event].mutex.RUnlock()

	for _, handle := range middleware.events[event].subscriptions {
		err := handle(message)
		if err != nil {
			Logger.Println(err)
			continue
		}
	}
}

// Subscribe creates a new middleware subscription for the given event type.
func (middleware *Middleware) Subscribe(event EventType, handle MiddlewareHandle) {
	if middleware.events[event] == nil {
		middleware.mutex.Lock()
		middleware.events[event] = &MiddlewareSubscriptions{subscriptions: []MiddlewareHandle{}}
		middleware.mutex.Unlock()
	}

	middleware.events[event].mutex.Lock()
	defer middleware.events[event].mutex.Unlock()

	middleware.events[event].subscriptions = append(middleware.events[event].subscriptions, handle)
}

// Use calles the given middleware controller to initialize the middleware
func (middleware *Middleware) Use(selector MiddlewareController) {
	selector(middleware.Subscribe)
}
