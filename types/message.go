package types

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/gofrs/uuid"
)

// Version message version
type Version int8

// String returns the version as a string
func (version Version) String() string {
	return strconv.Itoa(int(version))
}

// StatusCode represents an message status code.
// The status codes are base on the HTTP status code specifications.
type StatusCode int16

// String returns the StatusCode as a string
func (code StatusCode) String() string {
	return strconv.Itoa(int(code))
}

// Status codes that represents the status of a event
const (
	StatusOK                  StatusCode = 200
	StatusBadRequest          StatusCode = 400
	StatusUnauthorized        StatusCode = 401
	StatusForbidden           StatusCode = 403
	StatusNotFound            StatusCode = 404
	StatusConflict            StatusCode = 409
	StatusImATeapot           StatusCode = 418
	StatusInternalServerError StatusCode = 500
)

// MessageType represents a message type
type MessageType int8

// Available message types
const (
	EventMessage MessageType = iota + 1
	CommandMessage
)

// EOS end of stream
type EOS bool

// String returns a string representation of EOS
func (eos *EOS) String() string {
	if eos == nil || !*eos {
		return "0"
	}

	return "1"
}

// Parse parses the given string value to a EOS type
func (eos *EOS) Parse(value string) EOS {
	if value == "" || value == "0" {
		return false
	}

	return true
}

// NewMessage constructs a new message
func NewMessage(action string, version int8, key []byte, data []byte) *Message {
	// NOTE: take a look at other ways of generating id's
	id := uuid.Must(uuid.NewV4()).String()

	if key == nil {
		key = []byte(id)
	}

	return &Message{
		Ctx:     context.Background(),
		ID:      id,
		Action:  action,
		Version: Version(version),
		Key:     key,
		Data:    data,
		async:   make(chan struct{}, 0),
	}
}

// Message representation
type Message struct {
	ID        string          `json:"id"`
	Status    StatusCode      `json:"status"`
	Topic     Topic           `json:"topic"`
	Action    string          `json:"action"`
	Version   Version         `json:"version"`
	Data      []byte          `json:"data"`
	Key       []byte          `json:"key"`
	EOS       EOS             `json:"eos"`
	Timestamp time.Time       `json:"timestamp"`
	Ctx       context.Context `json:"-"`

	// NOTE: include message topic origin?
	schema interface{}
	async  chan struct{}
	result error
	once   sync.Once
	mutex  sync.RWMutex
}

// Schema returns the decoded message schema
func (message *Message) Schema() interface{} {
	return message.schema
}

// NewSchema overrides the message schema with the given value
func (message *Message) NewSchema(v interface{}) {
	message.schema = v
}

// NewError construct a new error message with the given message as parent
func (message *Message) NewError(action string, status StatusCode, err error) *Message {
	child := message.NewMessage(action, message.Version, message.Key, []byte(err.Error()))
	child.Status = status

	return child
}

// NewMessage construct a new event message with the given message as parent
func (message *Message) NewMessage(action string, version Version, key Key, data []byte) *Message {
	child := &Message{}
	child.Ctx = context.Background()

	child.Ctx = NewParentIDContext(child.Ctx, ParentID(message.ID))
	child.Ctx = NewParentTimestampContext(child.Ctx, ParentTimestamp(message.Timestamp))

	child.ID = uuid.Must(uuid.NewV4()).String()
	child.Action = action
	child.Status = StatusOK
	child.Data = data
	child.Timestamp = time.Now()
	child.Version = message.Version
	child.Key = key

	if child.Key == nil {
		child.Key = message.Key
	}

	if version == NullVersion {
		child.Version = message.Version
	}

	return child
}

// Reset set's up a new async resolver that awaits untill resolved
func (message *Message) Reset() {
	if message == nil {
		return
	}

	message.mutex.Lock()
	defer message.mutex.Unlock()

	message.once = sync.Once{}
	message.async = make(chan struct{}, 0)
	return
}

// Retry mark the message as resolved and attempt to retry the message
func (message *Message) Retry(err error) {
	if message == nil {
		return
	}

	message.resolve(err)
}

// Next mark the message as resolved
func (message *Message) Next() {
	if message == nil {
		return
	}

	message.resolve(nil)
}

func (message *Message) resolve(err error) {
	if message == nil {
		return
	}

	message.mutex.RLock()
	defer message.mutex.RUnlock()

	message.once.Do(func() {
		message.result = err
		close(message.async)
	})
}

// Await await untill the message is resolved
func (message *Message) Await() error {
	if message == nil {
		return nil
	}

	message.mutex.RLock()
	defer message.mutex.RUnlock()
	<-message.async
	return message.result
}
