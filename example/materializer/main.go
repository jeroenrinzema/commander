package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sysco-middleware/commander"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	uuid "github.com/satori/go.uuid"
)

var db *gorm.DB

// User gorm database table struct
type User struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
	ID        uuid.UUID `json:"id",gorm:"primary_key"`
	Username  *string   `json:"username"`
	Email     *string   `json:"email"`
}

// TableName table name of User
func (u *User) TableName() string {
	return "users"
}

func main() {
	db = openDatabase()
	db.AutoMigrate(&User{})

	server := newCommander()
	server.HandleEvent(handleEvent)
	server.ReadMessages()
}

func handleEvent(event *commander.Event) {
	switch event.Operation {
	case commander.CreateOperation:
		data := User{}
		ParseErr := json.Unmarshal(event.Data, &data)

		if ParseErr != nil {
			panic(ParseErr)
		}

		data.ID = event.Key
		db.Create(&data)
	case commander.UpdateOperation:
		data := new(map[string]interface{})
		ParseErr := json.Unmarshal(event.Data, &data)

		if ParseErr != nil {
			panic(ParseErr)
		}

		user := User{ID: event.Key}
		db.Model(&user).Updates(data)
	case commander.DeleteOperation:
		user := User{ID: event.Key}
		db.Delete(&user)
	}
}

func openDatabase() *gorm.DB {
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	database := os.Getenv("POSTGRES_DB")

	options := fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=disable", host, port, user, database, password)
	db, err := gorm.Open("postgres", options)

	if err != nil {
		panic(err)
	}

	// Close the database connection on SIGTERM
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		db.Close()
		os.Exit(0)
	}()

	return db
}

func newCommander() *commander.Commander {
	host := os.Getenv("KAFKA_HOST")
	group := os.Getenv("KAFKA_GROUP")

	instance := &commander.Commander{
		Producer: commander.NewProducer(host),
		Consumer: commander.NewConsumer(host, group),
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Close the kafka connection on SIGTERM
	go func() {
		<-sigs
		instance.Close()
		os.Exit(0)
	}()

	return instance
}
