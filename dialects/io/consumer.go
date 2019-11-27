package io

import (
	"bufio"
	"errors"
	"io"
	"time"
)

// ErrInvalidMessageDelimiter is returned when a invalid message delimiter is encountered
var ErrInvalidMessageDelimiter = errors.New("invalid message delimiter")

// ErrInvalidBufferSize is returned when a invalid buffer size is encountered
var ErrInvalidBufferSize = errors.New("invalid buffer size")

type Consumer struct {
	// BufferSize represents the amount of bytes read before a message chunk is returned.
	// If the buffer size is set to 0 bytes will be consumed until the message delimiter is reached or the reader is closed.
	BufferSize int
	// MessageDelimiter are the bytes representing the end of a message if no message delimiter is set a chunk will be send
	// once the configured buffer size is reached.
	MessageDelimiter byte
	// PollInterval is the configured interval which determines how long the reader sleeps when no bytes are read.
	// This interval avoids unnecessary load on the io.Reader.
	PollInterval time.Duration
}

// Read reads and sends message chunks over the returned channel.
// A chunk is determined by the configured buffer size or a message delimiter.
func (consumer *Consumer) Read(reader *bufio.Reader) chan []byte {
	sink := make(chan []byte)

	go func() {
		defer close(sink)

		for {
			var err error
			var bytes int
			var chunk []byte

			switch true {
			case consumer.BufferSize > 0:
				chunk, bytes, err = consumer.ReadBuffer(reader)
				break
			case consumer.MessageDelimiter != 0:
				chunk, bytes, err = consumer.ReadSlice(reader)
				break
			}

			if err != nil {
				// TODO: log error before returning
				return
			}

			if bytes == 0 {
				time.Sleep(consumer.PollInterval)
				continue
			}

			sink <- chunk
		}
	}()

	return sink
}

// ReadBuffer reads bytes from the given reader until the configured buffer size is reached
func (consumer *Consumer) ReadBuffer(reader *bufio.Reader) (chunk []byte, bytes int, err error) {
	if consumer.BufferSize == 0 {
		return chunk, 0, ErrInvalidBufferSize
	}

	chunk = make([]byte, consumer.BufferSize)
	bytes, err = reader.Read(chunk)

	if err != nil && !errors.Is(err, io.EOF) {
		return chunk, 0, err
	}

	return chunk, bytes, nil
}

// ReadSlice reads bytes from the given reader until a message delimiter or io.EOF is encountered
func (consumer *Consumer) ReadSlice(reader *bufio.Reader) (chunk []byte, bytes int, err error) {
	if consumer.MessageDelimiter == 0 {
		return chunk, 0, ErrInvalidMessageDelimiter
	}

	chunk, _ = reader.ReadSlice(consumer.MessageDelimiter)
	bytes = len(chunk)

	return chunk, bytes, nil
}
