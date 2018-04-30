package commands

import (
	"encoding/json"
	"fmt"

	uuid "github.com/satori/go.uuid"
	"github.com/sysco-middleware/commander"
)

// Create a new user
func Create(command *commander.Command) {
	type user struct {
		Username string `json:"username"`
		Email    string `json:"email"`
	}

	data := &user{}
	err := json.Unmarshal(command.Data, data)

	if err != nil {
		fmt.Println(err)
		return
	}

	res, _ := json.Marshal(data)
	id, _ := uuid.NewV4()

	event := command.NewEvent(EventAccountCreated, id, res)
	event.Produce()
}
