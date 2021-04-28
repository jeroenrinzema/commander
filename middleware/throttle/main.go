package throttle

import (
	"time"

	"github.com/jeroenrinzema/commander/internal/types"
)

// Throttle provides a middleware that limits the amount of messages processed per unit of time.
// This may be done e.g. to prevent excessive load caused by running a handler on a long queue of unprocessed messages.
type Throttle struct {
	count    int64
	interval time.Duration
}

// NewThrottle creates a new Throttle middleware.
// Example duration and count: NewThrottle(10, time.Second) for 10 messages per second
func NewThrottle(count int64, interval time.Duration) *Throttle {
	return &Throttle{
		count:    count,
		interval: interval,
	}
}

func (t Throttle) Middleware(h types.HandlerFunc) types.HandlerFunc {
	throttle := time.NewTicker(t.interval / time.Duration(t.count))

	return func(message *types.Message, writer types.Writer) {
		<-throttle.C
		// throttle is shared by multiple handlers, which will wait for their "tick"

		h(message, writer)
	}
}
