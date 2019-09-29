package types

// Dialect represents a commander dialect.
// A dialect is responsible for the consumption/production of the targeted protocol.
type Dialect interface {
	// Assigned notifies a dialect about the assignment of the given topic
	Assigned(topic Topic)

	// Consumer returns the dialect consumer
	Consumer() Consumer

	// Producer returns the dialect producer
	Producer() Producer

	// Healthy when called should it check if the dialect's consumer/producer are healthy and
	// up and running. This method could be called to check if the service is up and running.
	// The user should implement the health check
	Healthy() bool

	// Open notifies a dialect to open the dialect.
	// No further topic assignments should be made.
	Open() error

	// Close awaits till the consumer(s) and producer(s) of the given dialect are closed.
	// If an error is returned is the closing aborted and the error returned to the user.
	Close() error
}
