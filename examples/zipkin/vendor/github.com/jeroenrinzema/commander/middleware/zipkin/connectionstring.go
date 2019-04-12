package zipkin

import (
	"errors"
	"strings"
)

// ConnectionMap contains the connectionstring as a key/value map
type ConnectionMap map[string]string

// These const's contain the connection string keys to different values
const (
	ZipkinHost  = "host"
	ServiceName = "name"
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
	if values[ZipkinHost] == "" {
		return errors.New("no zipkin reporting host was given")
	}

	if values[ServiceName] == "" {
		return errors.New("no service name was defined")
	}

	return nil
}
