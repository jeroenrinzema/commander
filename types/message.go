package types

import (
	"context"
	"strconv"
	"time"
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

// Message a message
type Message struct {
	ID        string          `json:"id"`
	Topic     Topic           `json:"topic"`
	Action    string          `json:"action"`
	Version   Version         `json:"version"`
	Data      []byte          `json:"data"`
	Key       []byte          `json:"key"`
	EOS       bool            `json:"eos"`
	Timestamp time.Time       `json:"timestamp"`
	Ctx       context.Context `json:"-"`
}
