package kafka

import (
	"errors"
	"strings"
)

// ConnectionMap contains the connectionstring as a key/value map
type ConnectionMap map[string]string

// These const's contain the connection string keys to different values
const (
	BrokersKey           = "brokers"
	GroupKey             = "group"
	VersionKey           = "version"
	InitialOffsetKey     = "initial-offset"
	ConnectionTimeoutKey = "connection-timeout"
)

// ParseConnectionstring parses the given connectionstring and returns a map with all key/values
func ParseConnectionstring(connectionstring string) ConnectionMap {
	var values = make(map[string]string)

	pairs := strings.Split(connectionstring, " ")
	for _, pair := range pairs {
		keyval := strings.Split(pair, "=")
		if len(keyval) > 2 || len(keyval) < 2 {
			continue
		}

		key := keyval[0]
		value := keyval[1]

		values[key] = value
	}

	return values
}

// ValidateConnectionKeyVal validates if all required valyues are set in the given connectionmap
func ValidateConnectionKeyVal(values ConnectionMap) error {
	if len(values[BrokersKey]) == 0 {
		return errors.New("No brokers are defined in the connectionstring")
	}

	if len(values[VersionKey]) == 0 {
		return errors.New("No kafka version is defined in the connectionstring")
	}

	return nil
}
