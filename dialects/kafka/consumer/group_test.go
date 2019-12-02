package consumer

import (
	"testing"

	"github.com/Shopify/sarama"
)

func TestGroupHandleConnect(t *testing.T) {
	client, group, topics, broker, _ := NewMockClient(t)
	defer broker.Close()

	handle := NewGroupHandle(client)
	cluster, err := sarama.NewClient([]string{broker.Addr()}, NewMockConfig())
	if err != nil {
		t.Fatal(err)
	}

	go handle.Setup(nil)

	err = handle.Connect(cluster, topics, group)
	if err != nil {
		t.Fatal(err)
	}
}
