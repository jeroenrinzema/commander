package io

import (
	"bufio"
	"errors"
	"io"
	"time"

	"github.com/jeroenrinzema/commander/internal/types"
)

// ErrInvalidMessageDelimiter is returned when a invalid message delimiter is encountered
var ErrInvalidMessageDelimiter = errors.New("invalid message delimiter")

// ErrInvalidBufferSize is returned when a invalid buffer size is encountered
var ErrInvalidBufferSize = errors.New("invalid buffer size")

// NewConsumer constructs a new consumer for the given io.ReaderCloser
func NewConsumer(reader io.ReadCloser, marshaler Marshaler) *Consumer {
	return &Consumer{
		reader:    reader,
		marshaler: marshaler,
	}
}

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
	// ChunkChanSize represents the maximum length of the chunk sink channel.
	// This allows for fine grained memory control, the maximum memory allocated is the ChunkChanSize * BufferSize
	ChunkChanSize int

	reader    io.ReadCloser
	marshaler Marshaler
}

// Subscribe creates a new subscription and subscribes to the given topic(s)
func (consumer *Consumer) Subscribe(topics ...types.Topic) (subscription <-chan *types.Message, err error) {
	subscription = make(chan *types.Message, 0)

	return subscription, nil
}

// Unsubscribe removes the given subscription channel from any subscribed topics
func (consumer *Consumer) Unsubscribe(subscription <-chan *types.Message) error {
	return nil
}

// RemoveDelimiter removes the message delimiter from the given chunk if one is configured
func (consumer *Consumer) RemoveDelimiter(chunk []byte) {
	if consumer.MessageDelimiter > 0 {
		chunk = chunk[:len(chunk)-1]
	}
}

// Read reads and sends message chunks over the returned channel.
// A chunk is determined by the configured buffer size or a message delimiter.
func (consumer *Consumer) Read(reader *bufio.Reader) chan []byte {
	sink := make(chan []byte, consumer.ChunkChanSize)

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

// Close gracefully closes the given consumer
func (consumer *Consumer) Close() error {
	return nil
}
