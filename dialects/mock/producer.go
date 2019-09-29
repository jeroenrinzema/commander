package mock

import (
	"time"

	"github.com/jeroenrinzema/commander/internal/types"
	log "github.com/sirupsen/logrus"
)

// Producer a message producer
type Producer struct {
	consumer *Consumer
	logger   *log.Logger
}

// Publish produces a message to the given topic
func (producer *Producer) Publish(message *types.Message) error {
	producer.logger.Debug("publishing message")

	message.Timestamp = time.Now()
	go producer.consumer.Emit(message)
	return nil
}

// Close closes the producer
func (producer *Producer) Close() error {
	return nil
}
