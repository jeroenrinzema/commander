package commander

import uuid "github.com/satori/go.uuid"

// ResponseWriter is used by a event or command handler to construct
// a response.
type ResponseWriter interface {
	// AsyncEvent creates a new event message to the given group.
	// If a error occured while writing the event the the events topic(s).
	AsyncEvent(action string, version int, parent uuid.UUID, key uuid.UUID, data []byte) error

	// AsyncCommand creates a command message to the given group command topic
	// and does not await for the responding event.
	//
	// If no command key is set will the command id be used. A command key is used
	// to write a command to the right kafka partition therefor to guarantee the order
	// of the kafka messages is it important to define a "data set" key.
	AsyncCommand(action string, key uuid.UUID, data []byte) error

	// SyncCommand creates a command message to the given group and awaits
	// its responding event message. If no message is received within the set timeout period
	// will a timeout be thrown.
	SyncCommand(action string, key uuid.UUID, data []byte) error
}
