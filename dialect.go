package commander

// Dialect represents a commander dialect
type Dialect interface {
	// Open opens the given dialect with the given connectionstring
	Open(connectionstring string, groups ...*Group) (Consumer, Producer, error)
}
