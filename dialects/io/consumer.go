package io

import (
	"bufio"
	"io"
	"time"

	"github.com/jeroenrinzema/commander/internal/types"
	errors "golang.org/x/xerrors"
)

// ErrInvalidMessageDelimiter is returned when a invalid message delimiter is encountered
var ErrInvalidMessageDelimiter = errors.New("invalid message delimiter")

// ErrInvalidBufferSize is returned when a invalid buffer size is encountered
var ErrInvalidBufferSize = errors.New("invalid buffer size")

// Predefined default values
const (
	DefaultMessageDelimiter = byte('\n')
	DefaultPollInterval     = time.Second
)

// NewConsumer constructs a new consumer for the given io.ReaderCloser
func NewConsumer(reader io.ReadCloser, marshaller Marshaller) *Consumer {
	return &Consumer{
		reader:           reader,
		marshaller:       marshaller,
		MessageDelimiter: DefaultMessageDelimiter,
		PollInterval:     DefaultPollInterval,
	}
}

// Reader represents a chunk reader implementation of a io.Reader.
// Each returned chunk represents a message byte slice.
type Reader func() (chunks [][]byte, bytes int, err error)

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

	reader     io.ReadCloser
	marshaller Marshaller
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

// Read reads and sends message chunks over the returned channel.
// A chunk is determined by the configured buffer size or a message delimiter.
func (consumer *Consumer) Read(reader *bufio.Reader) chan []byte {
	sink := make(chan []byte, consumer.ChunkChanSize)

	go func() {
		defer close(sink)
		read := consumer.Reader(reader)

		for {
			chunks, bytes, err := read()
			for _, chunk := range chunks {
				sink <- chunk
			}

			if err != nil {
				// TODO: log error before returning
				return
			}

			if bytes == 0 {
				time.Sleep(consumer.PollInterval)
				continue
			}
		}
	}()

	return sink
}

// Reader constructs and returns a consumer reader for the predefined configurations.
// If no consumer buffer size is defined the default slice reader is returned.
func (consumer *Consumer) Reader(reader *bufio.Reader) Reader {
	if consumer.BufferSize > 0 {
		return consumer.BufferReader(reader)
	}

	return consumer.SliceReader(reader)
}

// BufferReader returns a buffer reader implementation for the given io.Reader
func (consumer *Consumer) BufferReader(reader *bufio.Reader) Reader {
	var remaining []byte

	return func() (chunks [][]byte, bytes int, err error) {
		chunk, bytes, err := consumer.ReadBuffer(reader)
		chunks, remaining = consumer.SplitChunk(append(remaining, chunk...))

		return chunks, bytes, err
	}
}

// SliceReader returns a slice reader implementation for the given io.Reader
func (consumer *Consumer) SliceReader(reader *bufio.Reader) Reader {
	return func() (chunks [][]byte, bytes int, err error) {
		chunk, bytes, err := consumer.ReadSlice(reader)

		chunks = make([][]byte, 1)
		chunks[0] = chunk

		return chunks, bytes, err
	}
}

// SplitChunk splits the given chunk into consumable message chunks based on the configured message delimiter.
func (consumer *Consumer) SplitChunk(chunk []byte) (returned [][]byte, remaining []byte) {
	if consumer.MessageDelimiter == 0 {
		return [][]byte{chunk}, make([]byte, 0)
	}

	returned = [][]byte{}
	position := 0

	for index := 0; index < len(chunk); index++ {
		if chunk[index] != consumer.MessageDelimiter {
			continue
		}

		message := chunk[position:index]
		if len(message) == 0 {
			continue
		}

		returned = append(returned, message)
		position = index + 1
	}

	if position >= len(chunk) {
		return returned, make([]byte, 0)
	}

	return returned, chunk[position:]
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
