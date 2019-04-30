package zipkin

import (
	"fmt"
	"testing"
)

// TestNewZipkinMiddleware tests if able to construct a new zipkin middleware instance
func TestNewZipkinMiddleware(t *testing.T) {
	host := "https://example.com"
	name := "mock"
	conn := fmt.Sprintf("host=%s name=%s", host, name)

	zip, err := New(conn)
	if err != nil {
		t.Fatal(err)
	}

	if zip == nil {
		t.Fatal("returned instance is nill")
	}
}
