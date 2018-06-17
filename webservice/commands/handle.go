package commands

import (
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jeroenrinzema/commander"
	"github.com/jeroenrinzema/commander/example/webservice/rest"
)

// Handle ...
func Handle(w http.ResponseWriter, r *http.Request) {
	res := rest.Response{ResponseWriter: w}
	params := r.URL.Query()
	vars := mux.Vars(r)

	sync := len(params["sync"]) > 0
	body, _ := ioutil.ReadAll(r.Body)

	action := vars["command"]
	command := commander.NewCommand(action, body)

	if sync {
		event, err := Commander.SyncCommand(command)

		if err != nil {
			res.SendPanic(err.Error(), command)
			return
		}

		res.SendOK(event)
		return
	}

	err := Commander.AsyncCommand(command)

	if err != nil {
		res.SendPanic(err.Error(), nil)
		return
	}

	res.SendCreated(command)
}
