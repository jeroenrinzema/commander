package commander

import (
	"sync"
	"sync/atomic"
)

// Breaker is an object that will allow conncurrent checks if the breaker is safe to use.
// This object works very much like a real world electrical breaker.
type Breaker struct {
	m    sync.Mutex
	open uint32
}

// Safe checks wheather the breaker is open or not
func (b *Breaker) Safe() bool {
	if atomic.LoadUint32(&b.open) == 1 {
		return false
	}

	return true
}

// Open opens the breaker thus making is unsafe
func (b *Breaker) Open() {
	b.m.Lock()
	defer b.m.Unlock()
	if b.open == 0 {
		atomic.StoreUint32(&b.open, 1)
	}
}

// Close closes the breaker thus making it safe
func (b *Breaker) Close() {
	b.m.Lock()
	defer b.m.Unlock()
	if b.open == 1 {
		atomic.StoreUint32(&b.open, 0)
	}
}
