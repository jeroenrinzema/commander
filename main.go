package commander

import (
	"errors"

	"github.com/jeroenrinzema/commander/internal/types"
	"github.com/jeroenrinzema/commander/middleware"
)

const (
	// DebugEnv os debug env key
	DebugEnv = "DEBUG"
)

const (
	// BeforeEvent gets called before a action gets taken.
	BeforeEvent = "before"
	// AfterEvent gets called after a action has been taken.
	AfterEvent = "after"
)

var (
	// ErrTimeout is returned when a timeout is reached when awaiting a responding event
	ErrTimeout = errors.New("timeout reached")
)

// NewClient constructs a new commander client.
// A client is needed to control a collection of groups.
func NewClient(groups ...*Group) (*Client, error) {
	middleware := middleware.NewClient()
	client := &Client{
		Use:    middleware,
		Groups: groups,
	}

	appendMiddleware(middleware, groups)
	topics := pullTopicsFromGroups(groups)
	dialects := groupTopicsByDialect(topics)

	for dialect, topics := range dialects {
		err := dialect.Open(topics)
		if err != nil {
			return nil, err
		}
	}

	return client, nil
}

// Client manages the consumers, producers and groups.
type Client struct {
	middleware.Use
	Groups []*Group
}

// Close closes the consumer and producer
func (client *Client) Close() error {
	dialects := make(map[types.Dialect]bool)

	for _, group := range client.Groups {
		for _, topic := range group.Topics {
			if dialects[topic.Dialect()] {
				continue
			}

			topic.Dialect().Close()
			dialects[topic.Dialect()] = true
		}
	}

	return nil
}

func pullTopicsFromGroups(groups []*Group) []types.Topic {
	returned := []types.Topic{}
	for _, group := range groups {
		returned = append(returned, group.Topics...)
	}

	return returned
}

func appendMiddleware(middleware middleware.Use, groups []*Group) {
	for _, group := range groups {
		group.Middleware = middleware
	}
}

func groupTopicsByDialect(topics []types.Topic) map[types.Dialect][]types.Topic {
	returned := map[types.Dialect][]types.Topic{}
	for _, topic := range topics {
		_, has := returned[topic.Dialect()]
		if !has {
			returned[topic.Dialect()] = []types.Topic{}
		}

		returned[topic.Dialect()] = append(returned[topic.Dialect()], topic)
	}

	return returned
}
