package consumer

import "github.com/Shopify/sarama"

// PartitionHandle represents a Sarama partition consumer
type PartitionHandle struct {
	client     *Client
	brokers    []string
	config     *sarama.Config
	partitions []int32
}
