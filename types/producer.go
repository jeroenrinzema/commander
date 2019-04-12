package types

// Producer a message producer
type Producer interface {
	// Publish produces a message to the given topic
	Publish(message *Message) error

	// Close closes the producer
	Close() error
}
