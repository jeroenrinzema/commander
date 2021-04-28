package options

import (
	"time"

	"github.com/jeroenrinzema/commander/internal/types"
)

const (
	// DefaultRetries represents the default amount of retry attempts
	DefaultRetries = 5
	// DefaultTimeout represents the default timeout when awaiting a "sync" command to complete
	DefaultTimeout = 5 * time.Second
)

// NewServerOptions applies the given serve options to construct a new server options definition
func NewServerOptions(options []ServerOption) (result *ServerOptions) {
	result = &ServerOptions{}
	for _, option := range options {
		option.Apply(result)
	}
	return result
}

// ServerOption sets options such as timeouts, codec and retries
type ServerOption interface {
	Apply(*ServerOptions)
}

// ServerOptions represent the available set of server options
type ServerOptions struct {
	Timeout time.Duration
	Retries int8
}

// NewGroupOptions applies the given serve options to construct a new group options definition
func NewGroupOptions(options []GroupOption) (result *GroupOptions) {
	result = &GroupOptions{
		Timeout: DefaultTimeout,
		Retries: DefaultRetries,
	}

	for _, option := range options {
		option.Apply(result)
	}

	return result
}

// GroupOption sets options such as topic definitions and timeouts
type GroupOption interface {
	Apply(*GroupOptions)
}

// GroupOptions represent the available set of group options
type GroupOptions struct {
	Timeout time.Duration
	Retries int8
	Topics  []types.Topic
}

// NewHandlerOptions applies the given serve options to construct a new handle options definition
func NewHandlerOptions(options []HandlerOption) (result *HandlerOptions) {
	// TODO: define default options
	result = &HandlerOptions{}
	for _, option := range options {
		option.Apply(result)
	}
	return result
}

// HandlerOption sets options such as codec interfaces and timeouts
type HandlerOption interface {
	Apply(*HandlerOptions)
}

// HandlerOptions represent the available set of handle options
type HandlerOptions struct {
	Action      string
	MessageType types.MessageType
	Callback    types.HandlerFunc
}
