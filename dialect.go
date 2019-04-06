package commander

// Dialect represents a commander dialect.
// A dialect is responsible for the consumption/production of the targeted protocol.
type Dialect interface {
	// Open opens the given dialect with the given connectionstring.
	// The dialect consumer and producer are initialized and the passed groups should be prepared for consumption/production.
	// Note that some groups could be marked for only consumption and/or only production. The user expects that only the given groups
	// are setup. If a error occured when parsing the connection string
	// or when opening the consumer/producer is it returned to the user.
	Open(connectionstring string, groups ...*Group) (Consumer, Producer, error)

	// Healthy when called should it check if the dialect's consumer/producer are healthy and
	// up and running. This method could be called to check if the service is up and running.
	// The user should implement the health check 
	Healthy() bool

	// Close awaits till the consumer(s) and producer(s) of the given dialect are closed.
	// If an error is returned is the closing aborted and the error returned to the user.
	Close() error
}
