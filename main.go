package commander

import (
	"errors"

	"github.com/jeroenrinzema/commander/middleware"
	"github.com/jeroenrinzema/commander/types"
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
	client := &Client{
		Groups:     groups,
		Middleware: middleware.NewClient(),
	}

	topics := []types.Topic{}
	dialects := []types.Dialect{}

	for _, group := range groups {
		group.Middleware = client.Middleware

		for _, t := range group.Topics {
			topics = append(topics, t...)
		}
	}

topic:
	for _, topic := range topics {
		for _, dialect := range dialects {
			if topic.Dialect() == dialect {
				continue topic
			}
		}

		dialects = append(dialects, topic.Dialect())
	}

	for _, dialect := range dialects {
		err := dialect.Open()
		if err != nil {
			return nil, err
		}
	}

	return client, nil
}

// Client manages the consumers, producers and groups.
type Client struct {
	Middleware *middleware.Client
	Groups     []*Group
}

// Close closes the consumer and producer
func (client *Client) Close() error {
	dialects := make(map[types.Dialect]bool)

	for _, group := range client.Groups {
		for _, topics := range group.Topics {
			for _, topic := range topics {
				if dialects[topic.Dialect()] {
					continue
				}

				topic.Dialect().Close()
				dialects[topic.Dialect()] = true
			}
		}
	}

	return nil
}
