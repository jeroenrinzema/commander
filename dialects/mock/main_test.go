package mock

import "testing"

// TestNewDialectConstruction tests if able to construct a new dialect
func TestNewDialectConstruction(t *testing.T) {
	dialect := NewDialect()

	dialect.Open(nil)

	if dialect.Consumer() == nil {
		t.Fatal("no dialect consumer")
	}

	if dialect.Producer() == nil {
		t.Fatal("no dialect producer")
	}

	if !dialect.Healthy() {
		t.Fatal("dialect not healthy")
	}

	err := dialect.Close()
	if err != nil {
		t.Fatal(err)
	}
}
