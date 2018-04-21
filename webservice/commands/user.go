package commands

import (
	"net/http"

	"github.com/sysco-middleware/commander/commander"
	"github.com/sysco-middleware/commander/webservice/rest"
)

// NewUser send the create user command
func NewUser(w http.ResponseWriter, r *http.Request) {
	req := rest.Request{Request: r, ResponseWriter: w}

	type user struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Email    string `json:"email"`
	}

	body := user{}
	req.GetJSONBody(&body)

	command := commander.NewCommand("new_user", body)
	handle(w, r, command)
}
