package main

import (
	"commander/rest/commander"
	"fmt"
	"net/http"

	uuid "github.com/satori/go.uuid"
)

func middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Authenticate a request
		fmt.Println("Incomming request:", r.URL)

		next.ServeHTTP(w, r)
	})
}

func main() {
	com := commander.Commander{
		Brokers: "localhost",
		Group:   "commands",
	}

	go com.ConsumeEvents()

	id, _ := uuid.NewV4()

	type user struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Email    string `json:"email"`
	}

	command := commander.Command{
		ID:     id,
		Action: "new_user",
		Data:   user{"john", "doeisthebest", "john@example.com"},
	}

	fmt.Println("Sending a sync command")

	res, err := com.SyncCommand(command)

	if err != nil {
		fmt.Println("Something went wrong:", err.Error())
		return
	}

	fmt.Println("Received response:", res)
}
