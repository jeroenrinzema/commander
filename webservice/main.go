package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/sysco-middleware/commander/webservice/websocket"

	"github.com/gorilla/mux"
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
	commander := commands.NewCommander()

	websocket.NewHub() // Create a new websocket hub to store all active connections
	router := routes()

	go websocket.Consume()
	go commander.ReadMessages()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		commander.Close()
		os.Exit(0)
	}()

	http.ListenAndServe(":8080", router)
}

func routes() *mux.Router {
	router := rest.Router()

	command := router.PathPrefix("/command/").Subrouter()
	command.HandleFunc("/new_user", rest.Use(commands.NewUser, authenticate)).Methods("POST")

	router.HandleFunc("/updates", rest.Use(websocket.Handle, authenticate)).Methods("GET")
	return router
}
