package mock

import (
	"sync"

	"github.com/jeroenrinzema/commander/internal/types"
)

// Subscription mock message subscription
type Subscription struct {
	messages chan *types.Message
}

// SubscriptionCollection represents a collection of subscriptions
type SubscriptionCollection struct {
	list  map[<-chan *types.Message]*Subscription
	mutex sync.Mutex
}

// NewTopic constructs a new subscription collection for a topic
func NewTopic() *SubscriptionCollection {
	return &SubscriptionCollection{
		list: map[<-chan *types.Message]*Subscription{},
	}
}
