package commander

import (
	"time"

	"github.com/jeroenrinzema/commander/internal/options"
	"github.com/jeroenrinzema/commander/internal/types"
)

type timeout struct {
	duration time.Duration
}

func (t *timeout) Apply(options *options.GroupOptions) {
	options.Timeout = t.duration
}

// WithAwaitTimeout returns a GroupOption that configures the timeout period for the given group
func WithAwaitTimeout(d time.Duration) options.GroupOption {
	return &timeout{d}
}

type action struct {
	name string
}

func (a *action) Apply(options *options.HandlerOptions) {
	options.Action = a.name
}

// WithAction returns a HandleOptions that configures the action handle
func WithAction(n string) options.HandlerOption {
	return &action{n}
}

type messageType struct {
	value types.MessageType
}

func (t *messageType) Apply(options *options.HandlerOptions) {
	options.MessageType = t.value
}

// WithMessageType returns a HandleOptions that configures the message type handle
func WithMessageType(t types.MessageType) options.HandlerOption {
	return &messageType{t}
}

type callback struct {
	handle types.HandlerFunc
}

func (c *callback) Apply(options *options.HandlerOptions) {
	options.Callback = c.handle
}

// WithCallback returns a HandleOptions that configures the callback method for a given handle
func WithCallback(h types.HandlerFunc) options.HandlerOption {
	return &callback{h}
}
