package main

import (
	"commander/rest/commander"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

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

	com.OpenProducer()
	com.ConsumeEvents()

	start := time.Now()
	for i := 0; i < 50; i++ {
		send(&com)
	}
	elapsed := time.Since(start)
	log.Printf("Took %s", elapsed)
}

func send(com *commander.Commander) {
	id, _ := uuid.NewV4()

	type user struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Email    string `json:"email"`
	}

	data, _ := json.Marshal(user{"john", "doeisthebest", "john@example.com"})
	command := commander.Command{
		ID:     id,
		Action: "new_user",
		Data:   string(data),
	}

	res, err := com.SyncCommand(command)

	if err != nil {
		fmt.Println("Something went wrong:", err.Error())
		return
	}

	fmt.Println("Received response:", res)
}
