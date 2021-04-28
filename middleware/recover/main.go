package recover

import "github.com/jeroenrinzema/commander/internal/types"

type Controller struct{}

func (controller *Controller) Middleware(next types.HandlerFunc) types.HandlerFunc {
	return func(message *types.Message, writer types.Writer) {
		defer func() {
			_ = recover()
		}()

		next(message, writer)
	}
}
