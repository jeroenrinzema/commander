package kafka

import (
	"fmt"
	"testing"
)

// TestParsingConnectionstring tests if able to parse a connectionstring
func TestParsingConnectionstring(t *testing.T) {
	val := "val"
	str := fmt.Sprintf("brokers=%s group=%s version=%s initial-offset=%s connection-timeout=%s", val, val, val, val, val)

	values := ParseConnectionstring(str)
	keys := []string{
		BrokersKey,
		GroupKey,
		VersionKey,
		InitialOffsetKey,
		ConnectionTimeoutKey,
	}

	for _, key := range keys {
		if values[key] != val {
			t.Fatalf("Key value not set: %s", key)
		}
	}
}

// TestConnectionValuesValidation tests if a given connectionstring map is validated correctly
func TestConnectionValuesValidation(t *testing.T) {
	val := "val"
	options := []string{
		fmt.Sprintf("brokers=%s version=%s", val, val),
		fmt.Sprintf("version=%s brokers=%s", val, val),
	}

	for _, option := range options {
		values := ParseConnectionstring(option)
		err := ValidateConnectionKeyVal(values)

		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestConnectionValuesValidationFailure tests if a given connectionstring map is validated correctly
func TestConnectionValuesValidationFailure(t *testing.T) {
	val := "val"
	options := []string{
		fmt.Sprintf("brokers=%s", val),
		fmt.Sprintf("version=%s", val),
	}

	for _, option := range options {
		values := ParseConnectionstring(option)
		err := ValidateConnectionKeyVal(values)

		if err == nil {
			t.Fatal("No error was thrown")
		}
	}
}
