package commands

import (
	"net/http"

	"github.com/sysco-middleware/commander/commander"
	"github.com/sysco-middleware/commander/webservice/rest"
)

func handle(w http.ResponseWriter, r *http.Request, command commander.Command) {
	res := rest.Response{ResponseWriter: w}
	params := r.URL.Query()
	sync := len(params["sync"]) > 0

	if sync {
		event, err := commander.Server.SyncCommand(command)

		if err != nil {
			res.SendPanic(err.Error(), command)
			return
		}

		res.SendOK(event)
		return
	}

	err := commander.Server.AsyncCommand(command)

	if err != nil {
		res.SendPanic(err.Error(), nil)
		return
	}

	res.SendCreated(command)
}
