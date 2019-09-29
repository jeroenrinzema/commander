package mock

import (
	"github.com/jeroenrinzema/commander/internal/types"
)

// NewDialect constructs a new in-memory mocking dialect
func NewDialect() types.Dialect {
	consumer := &Consumer{
		subscriptions: make(map[string]*SubscriptionCollection),
	}

	producer := &Producer{
		consumer: consumer,
	}

	dialect := &Dialect{
		consumer: consumer,
		producer: producer,
	}

	return dialect
}

// Dialect a in-memory mocking dialect
type Dialect struct {
	consumer *Consumer
	producer *Producer
}

// Assigned notifies a dialect about the assignment of the given topic
func (dialect *Dialect) Assigned(types.Topic) {
	// ignore...
}

// Consumer returns the dialect consumer
func (dialect *Dialect) Consumer() types.Consumer {
	return dialect.consumer
}

// Producer returns the dialect producer
func (dialect *Dialect) Producer() types.Producer {
	return dialect.producer
}

// Healthy when called should it check if the dialect's consumer/producer are healthy and
// up and running. This method could be called to check if the service is up and running.
// The user should implement the health check
func (dialect *Dialect) Healthy() bool {
	return true
}

// Close awaits till the consumer(s) and producer(s) of the given dialect are closed.
// If an error is returned is the closing aborted and the error returned to the user.
func (dialect *Dialect) Close() error {
	dialect.consumer.Close()
	dialect.producer.Close()
	return nil
}
