package zipkin

import (
	"fmt"
	"testing"
)

// TestParsingConnectionstring tests if able to parse a connectionstring
func TestParsingConnectionstring(t *testing.T) {
	val := "val"
	str := fmt.Sprintf("host=%s name=%s", val, val)

	values := ParseConnectionstring(str)

	if values[ZipkinHost] != val {
		t.Fatal("ZipkinHost is not set in value map")
	}

	if values[ServiceName] != val {
		t.Fatal("ServiceName is not set in value map")
	}
}

// TestConnectionValuesValidation tests if a given connectionstring map is validated correctly
func TestConnectionValuesValidation(t *testing.T) {
	val := "val"
	options := []string{
		fmt.Sprintf("host=%s name=%s", val, val),
		fmt.Sprintf("name=%s host=%s", val, val),
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
		fmt.Sprintf("host=%s", val),
		fmt.Sprintf("name=%s", val),
	}

	for _, option := range options {
		values := ParseConnectionstring(option)
		err := ValidateConnectionKeyVal(values)

		if err == nil {
			t.Fatal("No error was thrown")
		}
	}
}
