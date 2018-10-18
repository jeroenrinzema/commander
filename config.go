package commander

import (
	"errors"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// NewConfig initializes and returns a config struct.
func NewConfig() Config {
	config := &config{
		Timeout: 5 * time.Second,
	}

	return config
}

// Config validates and processes configuration options
type Config interface {
	// ValidateGroup validates the given group and returns a error if the group is invalid
	ValidateGroup(*Group) error

	// AddGroups validates and registeres the given groups at the producer and consumer
	AddGroups(...*Group) error
}

// config contains all config options for a commander instance.
type config struct {
	Timeout time.Duration
	Kafka   *kafka.ConfigMap
	Brokers []string
	Groups  []*Group
	mutex   sync.Mutex
}

func (config *config) ValidateGroup(group *Group) error {
	if len(group.CommandTopic.Name) == 0 {
		return errors.New("The given group has no command topic name set")
	}

	if len(group.EventTopic.Name) == 0 {
		return errors.New("The given group has no event topic name set")
	}

	return nil
}

func (config *config) AddGroups(groups ...*Group) error {
	for _, group := range groups {
		err := config.ValidateGroup(group)
		if err != nil {
			return err
		}
	}

	config.mutex.Lock()
	defer config.mutex.Unlock()

	config.Groups = append(config.Groups, groups...)
	return nil
}
