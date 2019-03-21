package consumer

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// NewPartitionHandle initializes a new PartitionHandle
func NewPartitionHandle(client *Client) *PartitionHandle {
	handle := &PartitionHandle{
		client:     client,
		ready:      make(chan bool, 0),
		partitions: make(map[string]*TopicPartitionConsumers),
	}

	return handle
}

// PartitionConsumer represents a single partition consumer
type PartitionConsumer struct {
	partition int32
	consumer  sarama.PartitionConsumer
}

// TopicPartitionConsumers represents a topic and it's partition consumers
type TopicPartitionConsumers struct {
	consumers []*PartitionConsumer
	highest   int32
	mutex     sync.RWMutex
}

// PartitionHandle represents a Sarama partition consumer
type PartitionHandle struct {
	client        *Client
	initialOffset int64
	topics        []string
	consumer      sarama.Consumer
	partitions    map[string]*TopicPartitionConsumers
	consumptions  sync.WaitGroup
	config        *sarama.Config
	mutex         sync.RWMutex
	ready         chan bool
}

// Heartbeat set's up a new time ticker that checks every time if the partition count
// has changed for the consumed topics. By default does the heartbeat tick every 1500ms
func (handle *PartitionHandle) Heartbeat() {
	ticker := time.NewTicker(1500 * time.Millisecond)
	for range ticker.C {
		handle.Rebalance()
	}
}

// PullPartitions pulls the available partitions for the set topics.
// If the partition count has changed are the new partitions returned.
func (handle *PartitionHandle) PullPartitions(topic string) ([]int32, error) {
	new := []int32{}

	partitions, err := handle.consumer.Partitions(topic)
	if err != nil {
		return new, err
	}

	handle.mutex.RLock()
	defer handle.mutex.RUnlock()

	if handle.partitions[topic] == nil {
		new = append(new, partitions...)
		return new, nil
	}

	// Partition length is never bigger than a 32 bit int
	highest := int32(len(partitions))
	if highest == handle.partitions[topic].highest {
		return new, nil
	}

	// Partitions are not guaranteed to be returned in asc/desc order
	for index := len(partitions) - 1; index >= 0; index-- {
		partition := partitions[index]
		if partition > highest {
			new = append(new, partition)
		}
	}

	return new, nil
}

// PartitionConsumer set's up a new partition consumer for the given topic and partition
func (handle *PartitionHandle) PartitionConsumer(topic string, partition int32) error {
	handle.mutex.Lock()
	if handle.partitions[topic] == nil {
		handle.partitions[topic] = &TopicPartitionConsumers{
			consumers: []*PartitionConsumer{},
			highest:   partition,
		}
	}
	handle.mutex.Unlock()

	// TODO: reopen partition consumer when cluster is unavailable
	consumer, err := handle.consumer.ConsumePartition(topic, partition, handle.initialOffset)
	if err != nil {
		return err
	}

	pc := &PartitionConsumer{
		partition: partition,
		consumer:  consumer,
	}

	handle.partitions[topic].mutex.Lock()
	defer handle.partitions[topic].mutex.Unlock()

	handle.partitions[topic].consumers = append(handle.partitions[topic].consumers, pc)
	go handle.ClaimMessages(topic, partition, consumer)

	return nil
}

// ClaimMessages handles the claiming of consumed messages
func (handle *PartitionHandle) ClaimMessages(topic string, partition int32, consumer sarama.PartitionConsumer) {
	for message := range consumer.Messages() {
		handle.client.Claim(message)
	}

	// TODO: reopen partition consumer when cluster is unavailable
}

// Rebalance pulls the latest available topics and starts new partition consumers when nessasery.
func (handle *PartitionHandle) Rebalance() error {
	for _, topic := range handle.topics {
		partitions, err := handle.PullPartitions(topic)
		if err != nil {
			return err
		}

		for _, partition := range partitions {
			handle.PartitionConsumer(topic, partition)
		}
	}

	return nil
}

// Connect initializes a new Sarama partition consumer and awaits till the consumer
// group is set up and ready to consume messages.
func (handle *PartitionHandle) Connect(brokers []string, topics []string, initialOffset int64, config *sarama.Config) error {
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return err
	}

	handle.consumer = consumer
	handle.initialOffset = initialOffset
	handle.topics = topics
	handle.Rebalance()

	go handle.Heartbeat()

	return nil
}

// Close closes the given consumer and all topic partition consumers.
// First are all partition consumers closed before the client consumer is closed.
func (handle *PartitionHandle) Close() error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	for _, topic := range handle.partitions {
		topic.mutex.Lock()

		for _, partition := range topic.consumers {
			err := partition.consumer.Close()
			if err != nil {
				return err
			}
		}

		topic.mutex.Unlock()
	}

	err := handle.consumer.Close()
	if err != nil {
		return err
	}

	return nil
}
