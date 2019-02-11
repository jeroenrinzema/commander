package commander

import (
	"os"
	"os/signal"
	"syscall"
)

const (
	// BeforeEvent gets called before a action gets taken.
	BeforeEvent = "before"
	// AfterEvent gets called after a action has been taken.
	AfterEvent = "after"
)

// New creates a new commander instance with the given dialect, connectionstring and groups
func New(dialect Dialect, connectionstring string, groups ...*Group) (*Client, error) {
	consumer, producer, err := dialect.Open(connectionstring, groups...)
	if err != nil {
		return nil, err
	}

	client := &Client{
		Dialect:  dialect,
		Consumer: consumer,
		Producer: producer,
	}

	for _, group := range groups {
		group.Client = client
	}

	return client, nil
}

// Client manages the consumers, producers and groups.
type Client struct {
	Connectionstring string
	Dialect          Dialect
	Consumer         Consumer
	Producer         Producer
	Groups           []Group
}

// CloseOnSIGTERM closes the commander client when a SIGTERM signal is send
func (client *Client) CloseOnSIGTERM() {
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm
	client.Close()
}

// Close closes the consumer and producer
func (client *Client) Close() error {
	err := client.Dialect.Close()
	if err != nil {
		return err
	}

	return nil
}
