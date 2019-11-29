package circuit

import "sync"

// Ready is a object that will mark once a entity is ready
type Ready struct {
	mark  chan struct{}
	once  sync.Once
	mutex sync.Mutex
}

// Mark mark the instance as ready
func (r *Ready) Mark() {
	r.once.Do(func() {
		r.mutex.Lock()
		defer r.mutex.Unlock()

		if r.mark == nil {
			return
		}

		close(r.mark)
	})
}

// On returns a channel that get's closed once the instance is marked as ready
func (r *Ready) On() <-chan struct{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.mark == nil {
		r.mark = make(chan struct{}, 0)
	}

	return r.mark
}
