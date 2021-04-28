package types

// Writer handle implementation for a given group and message
type Writer interface {
	// Event creates and produces a new event to the assigned group.
	Event(action string, version int8, key []byte, data []byte) (*Message, error)

	// Error produces a new error event to the assigned group.
	Error(action string, err error) (*Message, error)

	// Command produces a new command to the assigned group.
	Command(action string, version int8, key []byte, data []byte) (*Message, error)
}
