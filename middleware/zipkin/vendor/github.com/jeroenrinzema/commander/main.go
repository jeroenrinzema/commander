package commander

import (
	"errors"
	"io/ioutil"
	"log"
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

var (
	// ErrTimeout is returned when a timeout is reached when awaiting a responding event
	ErrTimeout = errors.New("timeout reached")

	// LoggingPrefix holds the commander logging prefix
	LoggingPrefix = "[Commander] "

	// LoggingFlags holds the logging flag mode
	LoggingFlags = log.Ldate | log.Ltime | log.Lshortfile

	// Logger holds a io message logger
	Logger = log.New(ioutil.Discard, LoggingPrefix, LoggingFlags)
)

// New creates a new commander instance with the given dialect, connectionstring and groups.
// The dialect will be opened and a new logger will be set up that discards the logs by default.
func New(dialect Dialect, connectionstring string, groups ...*Group) (*Client, error) {
	if len(groups) == 0 {
		return nil, errors.New("no commander group was given")
	}

	Logger.Println("Opening commander dialect...")

	consumer, producer, err := dialect.Open(connectionstring, groups...)
	if err != nil {
		return nil, err
	}

	client := &Client{
		Dialect:  dialect,
		Consumer: consumer,
		Producer: producer,
		Middleware: &Middleware{
			events: make(map[EventType]*MiddlewareSubscriptions),
		},
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
	Middleware       *Middleware
	Groups           []Group
}

// CloseOnSIGTERM closes the commander client when a SIGTERM signal is send
func (client *Client) CloseOnSIGTERM() {
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm
	Logger.Println("Received SIGTERM signal")

	client.Close()
}

// Close closes the consumer and producer
func (client *Client) Close() error {
	Logger.Println("Closing commander client")

	err := client.Dialect.Close()
	if err != nil {
		return err
	}

	return nil
}
