package websocket

import (
	"encoding/json"
	"fmt"

	"github.com/sysco-middleware/commander/commander"
	"github.com/sysco-middleware/commander/webservice/commands"
)

// Consume kafka event messages
func Consume() {
	consumer := commands.Commander.Consume("events")

	for msg := range consumer.Messages {
		event := commander.Event{}
		json.Unmarshal(msg.Value, &event)

		fmt.Println(event)
	}
}
