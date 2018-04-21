package main

import (
	"fmt"
	"net/http"

	"github.com/sysco-middleware/commander/commander"
	"github.com/sysco-middleware/commander/webservice/commands"
	"github.com/sysco-middleware/commander/webservice/rest"
)

func authenticate(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Incomming request:", r.URL)
		next.ServeHTTP(w, r)
	})
}

func main() {
	server := commander.NewServer(&commander.Config{
		Brokers: "localhost",
		Group:   "commands",
	})

	server.OpenProducer()
	go server.ConsumeEvents()

	router := rest.Router()
	command := router.PathPrefix("/command/").Subrouter()

	command.HandleFunc("/new_user", rest.Use(commands.NewUser, authenticate)).Methods("POST")

	http.ListenAndServe(":8080", router)
}
