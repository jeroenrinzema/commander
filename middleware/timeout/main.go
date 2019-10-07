package timeout

import (
	"context"
	"time"

	"github.com/jeroenrinzema/commander/internal/types"
)

// func New() middleware.Controller {
// }

type Controller struct {
}

// Middleware ...
func (c *Controller) Middleware(next types.HandlerFunc) types.HandlerFunc {
	return func(message *types.Message, writer types.Writer) {
		ctx, cancel := context.WithTimeout(message.Ctx(), time.Second)
		defer cancel()

		message.NewCtx(ctx)
		next(message, writer)
	}
}
