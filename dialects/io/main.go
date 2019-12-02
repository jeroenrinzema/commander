package io

import (
	"io"

	"github.com/jeroenrinzema/commander/internal/types"
)

// NewDialect constructs a new io reader writer dialect for the given interfaces
func NewDialect(reader io.Reader, writer io.Writer) *Dialect {
	return &Dialect{
		reader: reader,
		writer: writer,
	}
}

// Dialect a io reader/writer dialect
type Dialect struct {
	reader io.Reader
	writer io.Writer
}

// Open notifies a dialect to open the dialect.
// No further topic assignments should be made.
func (dialect *Dialect) Open([]types.Topic) error {
	return nil
}

// // Consumer returns the dialect consumer
// func (dialect *Dialect) Consumer() types.Consumer {
// 	return dialect.consumer
// }
//
// // Producer returns the dialect producer
// func (dialect *Dialect) Producer() types.Producer {
// 	return dialect.producer
// }

// Healthy when called should it check if the dialect's consumer/producer are healthy and
// up and running. This method could be called to check if the service is up and running.
// The user should implement the health check
func (dialect *Dialect) Healthy() bool {
	return true
}

// Close awaits till the consumer(s) and producer(s) of the given dialect are closed.
// If an error is returned is the closing aborted and the error returned to the user.
func (dialect *Dialect) Close() error {

	return nil
}
