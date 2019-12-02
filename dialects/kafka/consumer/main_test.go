package consumer

import (
	"testing"

	"github.com/Shopify/sarama"
)

// NewMockClient constructs a new mock client using the default configurations.
// The constructed client, topics, broker and responder are returned to be used inside the test case.
func NewMockClient(t *testing.T) (client *Client, group string, topics []string, broker *sarama.MockBroker, responder *sarama.MockFetchResponse) {
	topic := "mock"
	broker, responder = NewMockBroker(t, 0, topic)

	topics = []string{topic}
	brokers := []string{broker.Addr()}
	group = "mock"

	client = NewClient(brokers, group)

	return client, group, topics, broker, responder
}

// NewMockBroker constructs a new sarama mock broker.
// The constructed mock broker and responder are returned which could be used for further configuration.
// A partition is created for the given topic. The broker is assigned as leader for the given topic + partition.
func NewMockBroker(t *testing.T, partition int32, topic string) (*sarama.MockBroker, *sarama.MockFetchResponse) {
	broker := sarama.NewMockBroker(t, 0)
	responder := sarama.NewMockFetchResponse(t, 1)

	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(topic, partition, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(topic, partition, sarama.OffsetOldest, 0).
			SetOffset(topic, partition, sarama.OffsetNewest, 1000),
		"FetchRequest": responder,
	})

	return broker, responder
}

// NewMockConfig constructs a new predefined Sarama mock configuration.
// This configuration contains predefined values such as the cluster version.
func NewMockConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0

	return config
}
