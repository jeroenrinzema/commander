package circuit

import (
	"context"
	"testing"
	"time"
)

func TestReadyOnce(t *testing.T) {
	ready := Ready{}
	timeout, _ := context.WithTimeout(context.Background(), 50*time.Millisecond)

	go ready.Mark()

	select {
	case <-timeout.Done():
		t.Error("timeout reached")
	case <-ready.On():
	}
}

func TestReadyOnceMultipleMark(t *testing.T) {
	ready := Ready{}
	timeout, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	ready.Mark()
	ready.Mark()

	select {
	case <-timeout.Done():
		t.Error("timeout reached")
	case <-ready.On():
	}
}

func TestReadyOnceMultipleListeners(t *testing.T) {
	ready := Ready{}

	listener := func() {
		timeout, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		select {
		case <-timeout.Done():
			t.Error("timeout reached")
		case <-ready.On():
		}
	}

	ready.Mark()

	listener()
	listener()
}
