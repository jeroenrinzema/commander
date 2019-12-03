package io

import "github.com/jeroenrinzema/commander/internal/types"

// join a small helper function concatenating multiple byte slices into a single byte slice
func join(slices ...[]byte) (joined []byte) {
	for _, slice := range slices {
		joined = append(joined, slice...)
	}
	return joined
}

// NewBenchmarkMarshal constructs a new benchmark marshal implementation and returns the given byte message when Marshal is called
func NewBenchmarkMarshal(message []byte) *BenchmarkMarshal {
	return &BenchmarkMarshal{
		message: message,
	}
}

// BenchmarkMarshal is a mocking structure used to create accurate benchmarks without a minimum marshal overhead
type BenchmarkMarshal struct {
	message []byte
}

// Unmarshal attempts to decode the given bytes into a types.Message
func (b *BenchmarkMarshal) Unmarshal(chunk []byte) (*types.Message, error) {
	return &types.Message{}, nil
}

// Marshal attempts to encode the given message into a slice of bytes
func (b *BenchmarkMarshal) Marshal(msg *types.Message) ([]byte, error) {
	return b.message, nil
}
