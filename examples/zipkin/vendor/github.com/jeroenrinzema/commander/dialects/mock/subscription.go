package mock

import (
	"sync"

	"github.com/jeroenrinzema/commander/internal/types"
)

// Subscription mock message subscription
type Subscription struct {
	messages chan *types.Message
	marked   chan error
}

// SubscriptionCollection represents a collection of subscriptions
type SubscriptionCollection struct {
	list  []*Subscription
	mutex sync.RWMutex
}
