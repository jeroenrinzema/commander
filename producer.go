package commander

// Producer represents a dialect producer
type Producer interface {
	// Publish produces a message to the given topic
	Publish(message *Message) error

	// Close closes the producer
	Close() error
}
