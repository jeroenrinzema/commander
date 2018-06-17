package main

import (
	"fmt"
	"net/http"

	"github.com/jeroenrinzema/commander"
	"github.com/jeroenrinzema/commander/example/webservice/websocket"

	"github.com/gorilla/mux"
	"github.com/jeroenrinzema/commander/example/webservice/commands"
	"github.com/jeroenrinzema/commander/example/webservice/rest"
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

func routes() *mux.Router {
	router := rest.Router()

	router.HandleFunc("/command/{command}", rest.Use(commands.Handle, authenticate)).Methods("POST")
	router.HandleFunc("/updates", rest.Use(websocket.Handle, authenticate)).Methods("GET")
	return router
}
