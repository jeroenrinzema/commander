package commander

import (
	"errors"
	"io/ioutil"
	"log"
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

	// LoggingPrefix holds the commander logging prefix
	LoggingPrefix = "[Commander] "

	// LoggingFlags holds the logging flag mode
	LoggingFlags = log.Ldate | log.Ltime | log.Llongfile

	// Logger holds a io message logger
	Logger = log.New(ioutil.Discard, LoggingPrefix, LoggingFlags)
)

// NewClient constructs a new commander client.
// A client is needed to control a collection of groups.
func NewClient(groups ...*Group) *Client {
	client := &Client{
		Groups: groups,
		Middleware: &Middleware{
			events: make(map[EventType]*MiddlewareSubscriptions),
		},
	}

	for _, group := range groups {
		group.Middleware = client.Middleware
	}

	return client
}

// Client manages the consumers, producers and groups.
type Client struct {
	Middleware *Middleware
	Groups     []*Group
}

// Close closes the consumer and producer
func (client *Client) Close() error {
	Logger.Println("Closing commander client")

	// Close all dialects
	// Keep a list of dialects already closed (memory addresses)

	// err := client.Dialect.Close()
	// if err != nil {
	// 	return err
	// }

	return nil
}
