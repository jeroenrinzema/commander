package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jeroenrinzema/commander"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	uuid "github.com/satori/go.uuid"
)

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
	db := OpenDatabase()
	db.AutoMigrate(&User{})

	consumer := NewConsumer()
	consumer.Subscribe("events", nil)

	// Close the database and kafka connection on SIGTERM
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		consumer.Close()
		db.Close()
		os.Exit(0)
	}()

	for {
		msg, ReadErr := consumer.ReadMessage(-1)

		if ReadErr != nil {
			panic(ReadErr)
		}

		event := commander.Event{}
		PopulateErr := event.Populate(msg)

		if PopulateErr != nil {
			panic(PopulateErr)
		}

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
}

// NewConsumer create a new kafka consumer
func NewConsumer() *kafka.Consumer {
	host := os.Getenv("KAFKA_HOST")
	group := os.Getenv("KAFKA_GROUP")

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": host,
		"group.id":          group,
	})

	if err != nil {
		panic(err)
	}

	return consumer
}

// OpenDatabase open a new database connection
func OpenDatabase() *gorm.DB {
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

	return db
}
