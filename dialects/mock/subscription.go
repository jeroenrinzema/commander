package mock

import (
	"sync"

	"github.com/jeroenrinzema/commander"
)

// Subscription mock message subscription
type Subscription struct {
	messages chan *commander.Message
	marked   chan error
}

// SubscriptionCollection represents a collection of subscriptions
type SubscriptionCollection struct {
	list  []*Subscription
	mutex sync.RWMutex
}
