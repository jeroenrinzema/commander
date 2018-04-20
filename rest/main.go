package main

import (
	"event-driven-architecture/rest/commander"
	"fmt"
	"net/http"
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

	command := commander.Command{
		Action: "new_user",
	}

	go com.ConsumeEvents()
	com.SyncCommand(command)

	// server.Init()
	// server.Router.Use(middleware)
	//
	// err := http.ListenAndServe(":8080", server.Router)
	//
	// if err != nil {
	// 	panic(err)
	// }
}
