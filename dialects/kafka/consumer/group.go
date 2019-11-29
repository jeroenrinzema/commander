package consumer

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/jeroenrinzema/commander/internal/circuit"
	"github.com/sirupsen/logrus"
)

// NewGroupHandle initializes a new GroupHandle
func NewGroupHandle(client *Client) *GroupHandle {
	handle := &GroupHandle{
		client: client,
		ready:  circuit.Ready{},
	}

	return handle
}

// GroupHandle represents a Sarama consumer group consumer handle
type GroupHandle struct {
	client       *Client
	consumer     sarama.ConsumerGroup
	group        string
	consumptions sync.WaitGroup
	closing      bool
	ready        circuit.Ready
	mutex        sync.Mutex
}

// Connect initializes a new Sarama consumer group and awaits till the consumer
// group is set up and ready to consume messages.
func (handle *GroupHandle) Connect(conn sarama.Client, topics []string, group string) error {
	consumer, err := sarama.NewConsumerGroupFromClient(group, conn)
	if err != nil {
		return err
	}

	go func() {
		for {
			if handle.closing {
				break
			}

			ctx := context.Background()
			err := consumer.Consume(ctx, topics, handle)
			if err != nil {
				logrus.Error(err)
			}
		}
	}()

	select {
	case err := <-consumer.Errors():
		return err
	case <-handle.ready.On():
	}

	handle.consumer = consumer
	handle.group = group

	return nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
// This method is a implementation of the sarama consumer interface.
func (handle *GroupHandle) Setup(sarama.ConsumerGroupSession) error {
	handle.ready.Mark()
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (handle *GroupHandle) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// When a Kafka message is claimed is it passed to the client Claim method.
// If an error occurred during processing of the claimed message is the message marked to be retried.
func (handle *GroupHandle) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		handle.consumptions.Add(1)

		go func(message *sarama.ConsumerMessage) {
			err := handle.client.Claim(message)
			defer handle.consumptions.Done()

			if err == ErrRetry {
				// Mark the message to be consumed again
				session.ResetOffset(message.Topic, message.Partition, message.Offset, "")
				return
			}

			session.MarkMessage(message, "")
		}(message)
	}

	return nil
}

// Close closes the group consume handle and awaits till all claimed messages are processed.
// The consumer group get's marked for closing
func (handle *GroupHandle) Close() error {
	handle.closing = true
	err := handle.consumer.Close()
	if err != nil {
		return err
	}

	handle.consumptions.Wait()
	return nil
}
