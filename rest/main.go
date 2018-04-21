package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/sysco-middleware/commander/rest/commander"

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

	fmt.Println("Sending commands")

	com.OpenProducer()
	go com.ConsumeEvents()

	start := time.Now()
	for i := 0; i < 1000; i++ {
		// Send a new_email or new_user command
		r := rand.Intn(2)
		if r > 0 {
			newEmail(&com)
		} else {
			newUser(&com)
		}
	}

	elapsed := time.Since(start)
	log.Printf("Took %s", elapsed)
}

func newUser(com *commander.Commander) {
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

	_, err := com.SyncCommand(command)

	if err != nil {
		fmt.Println("Something went wrong:", err.Error())
		return
	}
}

func newEmail(com *commander.Commander) {
	id, _ := uuid.NewV4()

	type email struct {
		Email string `json:"email"`
	}

	data, _ := json.Marshal(email{"john@example.com"})
	command := commander.Command{
		ID:     id,
		Action: "new_email",
		Data:   string(data),
	}

	_, err := com.SyncCommand(command)

	if err != nil {
		fmt.Println("Something went wrong:", err.Error())
		return
	}
}
