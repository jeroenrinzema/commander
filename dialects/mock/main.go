package mock

import "github.com/jeroenrinzema/commander"

// Dialect represents the mock dialect
type Dialect struct {
}

// Open opens a mock consumer and producer
func (dialect *Dialect) Open(connectionstring string, groups ...*commander.Group) (*Consumer, *Producer, error) {
	consumer := &Consumer{}
	producer := &Producer{consumer}

	return consumer, producer, nil
}
