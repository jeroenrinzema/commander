package mock

import (
	"github.com/jeroenrinzema/commander/types"
)

// Subscription mock message subscription
type Subscription struct {
	messages chan *types.Message
	marked   chan error
}

// SubscriptionCollection represents a collection of subscriptions
type SubscriptionCollection struct {
	list map[<-chan *types.Message]*Subscription
}

// NewTopic constructs a new subscription collection for a topic
func NewTopic() *SubscriptionCollection {
	return &SubscriptionCollection{
		list: map[<-chan *types.Message]*Subscription{},
	}
}
