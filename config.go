package commander

import (
	"errors"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// NewConfig initializes and returns a config struct.
func NewConfig() *Config {
	config := &Config{
		Timeout: 5 * time.Second,
	}

	return config
}

// Config contains all config options for a commander instance.
type Config struct {
	Timeout time.Duration
	Kafka   *kafka.ConfigMap
	Brokers []string
	Groups  []*Group
	mutex   sync.Mutex
}

// ValidateGroup validates the given group and returns a error if the group is invalid
func (config *Config) ValidateGroup(group *Group) error {
	if len(group.CommandTopic.Name) == 0 {
		return errors.New("The given group has no command topic name set")
	}

	if len(group.EventTopic.Name) == 0 {
		return errors.New("The given group has no event topic name set")
	}

	return nil
}

// AddGroups validates and registeres the given groups at the producer and consumer
func (config *Config) AddGroups(groups ...*Group) error {
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
