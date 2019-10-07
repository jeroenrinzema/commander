package throttle

import (
	"time"

	"github.com/jeroenrinzema/commander/internal/types"
)

// Throttle provides a middleware that limits the amount of messages processed per unit of time.
// This may be done e.g. to prevent excessive load caused by running a handler on a long queue of unprocessed messages.
type Throttle struct {
	throttle <-chan time.Time
}

// NewThrottle creates a new Throttle middleware.
// Example duration and count: NewThrottle(10, time.Second) for 10 messages per second
func NewThrottle(count int64, duration time.Duration) *Throttle {
	return &Throttle{time.Tick(duration / time.Duration(count))}
}

func (t Throttle) Middleware(h types.HandlerFunc) types.HandlerFunc {
	return func(message *types.Message, writer types.Writer) {
		select {
		case <-t.throttle:
			// throttle is shared by multiple handlers, which will wait for their "tick"
		}

		h(message, writer)
	}
}
