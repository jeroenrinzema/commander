package io

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/jeroenrinzema/commander/internal/types"
	errors "golang.org/x/xerrors"
)

// Predefined default values
var (
	DefaultMessageDelimiter = []byte{'\r', '\n'}
	DefaultPollInterval     = time.Second
	DefaultBufferSize       = 64 * 1024
)

// NewConsumer constructs a new consumer for the given io.ReaderCloser
func NewConsumer(reader io.Reader, marshaller Marshaller) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		reader:           reader,
		marshaller:       marshaller,
		MessageDelimiter: DefaultMessageDelimiter,
		PollInterval:     DefaultPollInterval,
		subscriptions:    []chan *types.Message{},
		BufferSize:       DefaultBufferSize,
		ctx:              ctx,
		close:            cancel,
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
	MessageDelimiter []byte
	// PollInterval is the configured interval which determines how long the reader sleeps when no bytes are read.
	// This interval avoids unnecessary load on the io.Reader.
	PollInterval time.Duration
	// ChunkChanSize represents the maximum length of the chunk sink channel.
	// This allows for fine grained memory control, the maximum memory allocated is the ChunkChanSize * BufferSize
	ChunkChanSize int

	reader     io.Reader
	marshaller Marshaller

	subscriptions []chan *types.Message
	mutex         sync.RWMutex

	ctx   context.Context
	close context.CancelFunc
}

// Subscribe creates a new subscription and subscribes to the given topic(s)
func (consumer *Consumer) Subscribe(topics ...types.Topic) (<-chan *types.Message, error) {
	subscription := make(chan *types.Message, 0)

	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	consumer.subscriptions = append(consumer.subscriptions, subscription)
	return subscription, nil
}

// Unsubscribe removes the given subscription channel from any subscribed topics
func (consumer *Consumer) Unsubscribe(target <-chan *types.Message) error {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for index, subscription := range consumer.subscriptions {
		if subscription != target {
			continue
		}

		// Remove the given subscription from the stored subscriptions
		// The order of the subscriptions are not preserved to increase performance
		// https://stackoverflow.com/a/37335777
		consumer.subscriptions[len(consumer.subscriptions)-1], consumer.subscriptions[index] = consumer.subscriptions[index], consumer.subscriptions[len(consumer.subscriptions)-1]
		consumer.subscriptions = consumer.subscriptions[:len(consumer.subscriptions)-1]
		break
	}

	return nil
}

// Consume opens a new reader for the configured io.Reader
func (consumer *Consumer) Consume() {
	sink := make(chan []byte, consumer.ChunkChanSize)
	go consumer.Read(consumer.reader, sink)

	for chunk := range sink {
		message, err := consumer.marshaller.Unmarshal(chunk)
		if err != nil {
			// TODO: log/handle error (ignore io.EOF)
			continue
		}

		consumer.mutex.RLock()

		for _, subscription := range consumer.subscriptions {
			subscription <- message
		}

		consumer.mutex.RUnlock()
	}
}

// Read reads and sends message chunks over the passed channel.
// A chunk is determined by the configured buffer size or a message delimiter.
func (consumer *Consumer) Read(reader io.Reader, sink chan []byte) {
	defer close(sink)
	read := consumer.BufferReader(reader)

	for {
		if consumer.Closed() {
			return
		}

		chunks, length, err := read()
		for _, chunk := range chunks {
			sink <- chunk
		}

		if err != nil && !errors.Is(err, io.EOF) {
			// TODO: log error before returning
			return
		}

		if length == 0 {
			time.Sleep(consumer.PollInterval)
			continue
		}
	}
}

// BufferReader returns a buffer reader implementation for the given io.Reader
func (consumer *Consumer) BufferReader(reader io.Reader) Reader {
	var remaining []byte

	return func() (chunks [][]byte, _ int, _ error) {
		chunk := make([]byte, consumer.BufferSize)
		length, _ := reader.Read(chunk)
		chunks, remaining = consumer.SplitChunk(append(remaining, chunk...))

		return chunks, length, nil
	}
}

// dropCR drops a terminal \r from the data.
func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

func ScanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(data, []byte{'\r', '\n'}); i >= 0 {
		// We have a full newline-terminated line.
		return i + 2, dropCR(data[0:i]), nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), dropCR(data), nil
	}
	// Request more data.
	return 0, nil, nil
}

// SplitChunk splits the given chunk into consumable message chunks based on the configured message delimiter.
func (consumer *Consumer) SplitChunk(chunk []byte) (returned [][]byte, remaining []byte) {
	if consumer.MessageDelimiter == nil {
		return [][]byte{chunk}, make([]byte, 0)
	}

	returned = [][]byte{}
	position := 0

	for {
		// FIXME: unnecessary memory allocation?
		index := bytes.Index(chunk[position:], consumer.MessageDelimiter)
		if index < 0 {
			break
		}

		message := chunk[position : position+index]
		if len(message) == 0 {
			continue
		}

		returned = append(returned, message)
		position = position + index + len(consumer.MessageDelimiter)
	}

	if position >= len(chunk) {
		return returned, make([]byte, 0)
	}

	return returned, chunk[position:]
}

// Closed returns a boolean representing whether the given consumer is closed
func (consumer *Consumer) Closed() bool {
	if consumer.ctx.Err() == context.Canceled {
		return true
	}

	return false
}

// Close gracefully closes the given consumer
func (consumer *Consumer) Close() error {
	consumer.close()
	return nil
}
