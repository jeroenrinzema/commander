package types

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jeroenrinzema/commander/internal/metadata"
)

var (
	// ErrNegativeAcknowledgement is a error representing a negative message acknowledgement
	ErrNegativeAcknowledgement = errors.New("negative acknowledgement")
)

// Resolved represents a message ack/nack status
type Resolved int

// available Resolved types
const (
	UnkownResolvedStatus Resolved = iota
	ResolvedAck
	ResolvedNack
)

var closed = make(chan struct{})

func init() {
	close(closed)
}

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
		ID:        id,
		Action:    action,
		Version:   Version(version),
		Key:       key,
		Data:      data,
		ack:       make(chan struct{}, 0),
		nack:      make(chan struct{}, 0),
		response:  UnkownResolvedStatus,
		Status:    StatusOK,
		Timestamp: time.Now(),
		ctx:       context.Background(),
	}
}

// Message representation
type Message struct {
	ID        string     `json:"id"`
	Status    StatusCode `json:"status"`
	Topic     Topic      `json:"topic"`
	Action    string     `json:"action"`
	Version   Version    `json:"version"`
	Data      []byte     `json:"data"`
	Key       []byte     `json:"key"`
	EOS       EOS        `json:"eos"`
	Timestamp time.Time  `json:"timestamp"`

	ctx      context.Context
	schema   interface{}
	ack      chan struct{}
	nack     chan struct{}
	response Resolved
	mutex    sync.RWMutex
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
func (message *Message) NewMessage(action string, version Version, key metadata.Key, data []byte) *Message {
	if key == nil {
		key = message.Key
	}

	if version == NullVersion {
		version = message.Version
	}

	child := NewMessage(action, int8(version), key, data)
	child.ctx = metadata.NewParentIDContext(child.ctx, metadata.ParentID(message.ID))
	child.ctx = metadata.NewParentTimestampContext(child.ctx, metadata.ParentTimestamp(message.Timestamp))

	return child
}

// Reset set's up a new async resolver that awaits untill resolved
func (message *Message) Reset() {
	if message == nil {
		return
	}

	message.mutex.Lock()
	defer message.mutex.Unlock()

	message.ack = make(chan struct{}, 0)
	message.nack = make(chan struct{}, 0)
	message.response = UnkownResolvedStatus

	return
}

// Ack mark the message as acknowledged
func (message *Message) Ack() bool {
	if message == nil {
		return false
	}

	message.mutex.Lock()
	defer message.mutex.Unlock()

	if message.response == ResolvedNack {
		return false
	}

	message.response = ResolvedAck

	if message.ack == nil {
		message.ack = closed
		return true
	}

	close(message.ack)
	return true
}

// Acked returns a channel thet get's closed once a acknowledged signal got sent
func (message *Message) Acked() <-chan struct{} {
	return message.ack
}

// Nack send a negative acknowledged
func (message *Message) Nack() bool {
	if message == nil {
		return false
	}

	message.mutex.Lock()
	defer message.mutex.Unlock()

	if message.response == ResolvedAck {
		return false
	}

	message.response = ResolvedNack

	if message.nack == nil {
		message.nack = closed
		return true
	}

	close(message.nack)
	return true
}

// Nacked returns a channel that get's closed once a negative acknowledged signal got sent
func (message *Message) Nacked() <-chan struct{} {
	return message.nack
}

// Finally is returned once the message is resolved.
// A ErrNegativeAcknowledgement error is returned if the message got negative acknowledged.
func (message *Message) Finally() error {
	if message == nil {
		return nil
	}

	select {
	case <-message.Acked():
		return nil
	case <-message.Nacked():
		return ErrNegativeAcknowledgement
	}
}

// Ctx returns the message context.
// This method could safely be called concurrently.
func (message *Message) Ctx() context.Context {
	message.mutex.RLock()
	defer message.mutex.RUnlock()
	return message.ctx
}

// NewCtx updates the message context.
// This method could safely be called concurrently.
func (message *Message) NewCtx(ctx context.Context) {
	message.mutex.Lock()
	defer message.mutex.Unlock()
	message.ctx = ctx
}
