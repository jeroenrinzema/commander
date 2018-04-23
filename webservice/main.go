package main

import (
	"fmt"
	"net/http"

	"github.com/sysco-middleware/commander/commander"
	"github.com/sysco-middleware/commander/webservice/websocket"

	"github.com/gorilla/mux"
	"github.com/sysco-middleware/commander/webservice/commands"
	"github.com/sysco-middleware/commander/webservice/rest"
)

var server *commander.Commander

func authenticate(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Incomming request:", r.URL)
		next.ServeHTTP(w, r)
	})
}

func main() {
	server = commands.NewCommander()

	websocket.NewHub() // Create a new websocket hub to store all active connections
	router := routes()

	go websocket.Consume()
	go server.ReadMessages()

	http.ListenAndServe(":8080", router)
}

func health(w http.ResponseWriter, r *http.Request) {
	res := rest.Response{ResponseWriter: w}

	command := commander.NewCommand("ping", nil)
	event, err := server.SyncCommand(command)

	if err != nil {
		res.SendPanic(err.Error(), nil)
		return
	}

	res.SendOK(event)
}

func routes() *mux.Router {
	router := rest.Router()

	command := router.PathPrefix("/command/").Subrouter()
	command.HandleFunc("/new_user", rest.Use(commands.NewUser, authenticate)).Methods("POST")

	router.HandleFunc("/updates", rest.Use(websocket.Handle, authenticate)).Methods("GET")
	router.HandleFunc("/health", health).Methods("GET")
	return router
}
