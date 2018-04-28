package main

import (
	"fmt"
	"net/http"

	"github.com/spf13/viper"
	"github.com/sysco-middleware/commander"
	"github.com/sysco-middleware/commander/example/webservice/websocket"

	"github.com/gorilla/mux"
	"github.com/sysco-middleware/commander/example/webservice/commands"
	"github.com/sysco-middleware/commander/example/webservice/rest"
)

var server *commander.Commander

func authenticate(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Incomming request:", r.URL)
		next.ServeHTTP(w, r)
	})
}

func main() {
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./config")
	viper.SetConfigName("default")

	err := viper.ReadInConfig()

	if err != nil {
		panic(err)
	}

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
