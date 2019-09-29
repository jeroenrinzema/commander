package types

// Writer handle implementation for a given group and message
type Writer interface {
	// Event creates and produces a new event to the assigned group.
	// The produced event is marked as EOS (end of stream).
	Event(action string, version int8, key []byte, data []byte) (*Message, error)

	// EventStream creates and produces a new event to the assigned group.
	// The produced event is one of many events in the event stream.
	EventStream(action string, version int8, key []byte, data []byte) (*Message, error)

	// EventEOS alias of Event
	EventEOS(action string, version int8, key []byte, data []byte) (*Message, error)

	// Error produces a new error event to the assigned group.
	// The produced error event is marked as EOS (end of stream).
	Error(action string, status StatusCode, err error) (*Message, error)

	// ErrorStream produces a new error event to the assigned group.
	// The produced error is one of many events in the event stream.
	ErrorStream(action string, status StatusCode, err error) (*Message, error)

	// ErrorEOS alias of Error
	ErrorEOS(action string, status StatusCode, err error) (*Message, error)

	// Command produces a new command to the assigned group.
	// The produced error event is marked as EOS (end of stream).
	Command(action string, version int8, key []byte, data []byte) (*Message, error)

	// CommandStream produces a new command to the assigned group.
	// The produced comamnd is one of many commands in the command stream.
	CommandStream(action string, version int8, key []byte, data []byte) (*Message, error)

	// CommandEOS alias of Command
	CommandEOS(action string, version int8, key []byte, data []byte) (*Message, error)
}
