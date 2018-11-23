package commander

// Producer represents a dialect producer
type Producer interface {
	// Produce produces a message to the given topic
	Produce(message *Message) error

	// Close closes the producer
	Close() error
}
