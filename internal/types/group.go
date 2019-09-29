package types

// Close represents a closing method
type Close func()

// Next indicates that the next message could be called
type Next func()

// Handle message handle message, writer implementation
type Handle func(*Message, Writer)

// Handler interface handle wrapper
type Handler interface {
	Handle(*Message, Writer)
}
