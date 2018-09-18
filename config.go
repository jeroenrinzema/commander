package commander

import (
	"sync"
	"time"
)

// NewConfig initializes and returns a config struct.
func NewConfig() *Config {
	config := Config{
		Timeout: 5 * time.Second,
	}

	return &config
}

// Config contains all config options for a commander instance.
type Config struct {
	Timeout time.Duration
	Brokers []string
	Groups  []*Group
	mutex   sync.Mutex
}

// AddGroups registeres a commander group and initializes it with
// the set consumer and producer.
func (config *Config) AddGroups(groups ...*Group) {
	config.mutex.Lock()
	defer config.mutex.Unlock()

	config.Groups = append(config.Groups, groups...)
}
