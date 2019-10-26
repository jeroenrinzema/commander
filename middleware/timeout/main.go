package timeout

import (
	"context"
	"time"

	"github.com/jeroenrinzema/commander/internal/types"
)

// func New() middleware.Controller {
// }

// Controller provides a middleware handler managing request timeouts
type Controller struct {
}

// Middleware controller handling a timeout request
func (c *Controller) Middleware(next types.HandlerFunc) types.HandlerFunc {
	return func(message *types.Message, writer types.Writer) {
		ctx, cancel := context.WithTimeout(message.Ctx(), time.Second)
		defer cancel()

		message.NewCtx(ctx)
		next(message, writer)
	}
}
