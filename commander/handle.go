package commander

import uuid "github.com/satori/go.uuid"

// CommandCallback the callback function that is called when a command message is received
type CommandCallback func(command Command)

var handles = []*Handle{}

// CommandHandle create a new command handle that listens for the given action
func CommandHandle(action string) *Handle {
	handle := &Handle{Action: action}
	handle.Listen()
	return handle
}

// Handle handle a new event
type Handle struct {
	ID      uuid.UUID
	Action  string
	command chan Command
	event   chan Event
	quit    chan bool
}

// Listen add this handle to the handles slice
func (h *Handle) Listen() {
	h.command = make(chan Command, 10)
	h.event = make(chan Event, 10)

	handles = append(handles, h)
}

// Start add this handle to the handles slice
func (h *Handle) Start(callback CommandCallback) {
	go func() {
		for {
			select {
			case command := <-h.command:
				callback(command)
			case <-h.quit: // will explain this in the last section
				return
			}
		}
	}()

	handles = append(handles, h)
}

// Close remove the handle from the handles slice
func (h *Handle) Close() {
	for index, handle := range handles {
		if handle == h {
			handles = append(handles[:index], handles[index+1:]...)
		}
	}
}
