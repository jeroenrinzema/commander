package commander

// Dialect represents a commander dialect
type Dialect interface {
	// Open opens the given dialect with the given connectionstring
	Open(connectionstring string, groups ...*Group) (Consumer, Producer, error)

	// Healthy checks if the dialect is healthy and up and running
	Healthy() bool

	// Close closes the dialect and it't consumers/producers
	Close() error
}
