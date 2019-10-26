package zipkin

import (
	"fmt"
	"testing"

	"github.com/jeroenrinzema/commander"
)

func MockConn() string {
	host := "https://example.com"
	name := "mock"
	conn := fmt.Sprintf("host=%s name=%s", host, name)
	return conn
}

// TestNewZipkinMiddleware tests if able to construct a new zipkin middleware instance
func TestNewZipkinMiddleware(t *testing.T) {
	conn := MockConn()
	zip, err := New(conn)
	if err != nil {
		t.Fatal(err)
	}

	if zip == nil {
		t.Fatal("returned instance is nill")
	}
}

func TestBeforeConsume(t *testing.T) {
	conn := MockConn()
	zip, err := New(conn)
	if err != nil {
		t.Fatal(err)
	}

	message := commander.NewMessage("example", 1, nil, nil)
	writer := commander.NewWriter(nil, nil)

	next := func(message *commander.Message, writer commander.Writer) {}
	zip.BeforeConsume(next)(message, writer)
}

func TestBeforeProduce(t *testing.T) {
	conn := MockConn()
	zip, err := New(conn)
	if err != nil {
		t.Fatal(err)
	}

	message := commander.NewMessage("example", 1, nil, nil)

	next := func(message *commander.Message) {}
	zip.BeforeProduce(next)(message)
}

func TestBeforeProduceWithParent(t *testing.T) {
	conn := MockConn()
	zip, err := New(conn)
	if err != nil {
		t.Fatal(err)
	}

	message := commander.NewMessage("example", 1, nil, nil)
	writer := commander.NewWriter(nil, nil)

	zip.BeforeConsume(func(message *commander.Message, writer commander.Writer) {})(message, writer)
	zip.BeforeProduce(func(message *commander.Message) {})(message)
}
