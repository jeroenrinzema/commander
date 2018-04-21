package commander

// CommandCallback the callback function that is called when a command message is received
type CommandCallback func(command Command)

var handles = []*Handle{}

// CommandHandle create a new command handle that listens for the given action
func CommandHandle(action string) *Handle {
	return &Handle{Action: action}
}

// Handle handle a new command
type Handle struct {
	Action string
	source chan Command
	quit   chan bool
}

// Start add this handle to the handles slice
func (h *Handle) Start(callback CommandCallback) {
	h.source = make(chan Command, 10)

	go func() {
		for {
			select {
			case command := <-h.source:
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
