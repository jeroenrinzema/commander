package commander

import (
	"time"

	"github.com/jeroenrinzema/commander/types"
)

type timeout struct {
	duration time.Duration
}

func (t *timeout) Apply(options *types.GroupOptions) {
	options.Timeout = t.duration
}

// WithAwaitTimeout returns a GroupOption that configures the timeout period for the given group
func WithAwaitTimeout(d time.Duration) types.GroupOption {
	return &timeout{d}
}

type action struct {
	name string
}

func (a *action) Apply(options *types.HandleOptions) {
	options.Action = a.name
}

// WithAction returns a HandleOptions that configures the action handle
func WithAction(n string) types.HandleOption {
	return &action{n}
}

type messageType struct {
	value types.MessageType
}

func (t *messageType) Apply(options *types.HandleOptions) {
	options.MessageType = t.value
}

// WithMessageType returns a HandleOptions that configures the message type handle
func WithMessageType(t types.MessageType) types.HandleOption {
	return &messageType{t}
}

type callback struct {
	handle types.Handle
}

func (c *callback) Apply(options *types.HandleOptions) {
	options.Callback = c.handle
}

// WithCallback returns a HandleOptions that configures the callback method for a given handle
func WithCallback(h types.Handle) types.HandleOption {
	return &callback{h}
}

type schema struct {
	handle func() interface{}
}

func (s *schema) Apply(options *types.HandleOptions) {
	options.Schema = s.handle
}

// WithMessageSchema returns a HandleOptions that configures the message schema for a handle
func WithMessageSchema(f func() interface{}) types.HandleOption {
	return &schema{f}
}
