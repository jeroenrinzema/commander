package types

import "time"

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
	result = &GroupOptions{}
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
	Codec   Codec
	Retries int8
	Topics  []Topic
}

// NewHandleOptions applies the given serve options to construct a new handle options definition
func NewHandleOptions(options []HandleOption) (result *HandleOptions) {
	// TODO: define default options
	result = &HandleOptions{}
	for _, option := range options {
		option.Apply(result)
	}
	return result
}

// HandleOption sets options such as codec interfaces and timeouts
type HandleOption interface {
	Apply(*HandleOptions)
}

// HandleOptions represent the available set of handle options
type HandleOptions struct {
	Action      string
	MessageType MessageType
	Schema      func() interface{}
	Callback    Handle
}
