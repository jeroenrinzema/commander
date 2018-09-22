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
	mutex   sync.Mutex
}
