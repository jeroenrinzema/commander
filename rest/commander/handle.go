package commander

import uuid "github.com/satori/go.uuid"

var handles = []*Handle{}

// Handle handle a new event
type Handle struct {
	ID     uuid.UUID
	source chan Event
	quit   chan bool
}

// Start add this handle to the handles slice
func (h *Handle) Start() {
	h.source = make(chan Event, 10)
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
