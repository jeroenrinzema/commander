package mock

import "github.com/jeroenrinzema/commander"

// Producer produces kafka messages
type Producer struct {
	consumer *Consumer
}

// Publish publishes the given message
func (producer *Producer) Publish(message commander.Message) error {
	producer.consumer.Emit(message)
	return nil
}

// Close closes the kafka producer
func (producer *Producer) Close() error {
	return nil
}
