package io

import (
	"bytes"
	"testing"

	"github.com/jeroenrinzema/commander/internal/types"
)

func TestSubscribe(t *testing.T) {
	gob := &Gob{}
	expected := &types.Message{
		Data: []byte("test case"),
	}

	chunk, err := gob.Marshal(expected)
	if err != nil {
		t.Error(err)
	}

	bb := bytes.NewBuffer(join(chunk, DefaultMessageDelimiter))

	consumer := NewConsumer(bb, gob)
	defer consumer.Close()

	go consumer.Consume()

	subscription, err := consumer.Subscribe(nil)
	if err != nil {
		t.Error(err)
	}

	message := <-subscription

	if string(message.Data) != string(expected.Data) {
		t.Error("expected message data malformed")
	}
}

func TestStreaming(t *testing.T) {
	n := 100
	gob := &Gob{}
	message := &types.Message{
		Data: []byte("test case"),
	}

	chunk, err := gob.Marshal(message)
	if err != nil {
		t.Error(err)
	}

	bb := bytes.NewBuffer(nil)

	for i := 0; i < n; i++ {
		bb.Write(join(chunk, DefaultMessageDelimiter))
	}

	consumer := NewConsumer(bb, gob)
	subscription, _ := consumer.Subscribe(nil)

	defer consumer.Close()
	go consumer.Consume()

	for i := 0; i < n; i++ {
		<-subscription
	}
}

func TestUnsubscribe(t *testing.T) {
	consumer := NewConsumer(nil, nil)
	defer consumer.Close()

	subscription, err := consumer.Subscribe(nil)
	if err != nil {
		t.Error(err)
	}

	err = consumer.Unsubscribe(subscription)
	if err != nil {
		t.Error(err)
	}

	if len(consumer.subscriptions) > 0 {
		t.Error("subscription not unsubscribed")
	}
}

func TestSplitChunk(t *testing.T) {
	consumer := NewConsumer(nil, nil)
	defer consumer.Close()

	messages, remaining := consumer.SplitChunk(join([]byte{1}, DefaultMessageDelimiter, []byte{3, 4, 5}, DefaultMessageDelimiter))

	if len(messages) != 2 {
		t.Errorf("unexpected ammount of messages returned. Expected 2 returned %d", len(messages))
	}

	if len(remaining) > 0 {
		t.Error("unexpected remaining bytes returned")
	}

	if len(messages[0]) != 1 {
		t.Errorf("unexpected amount of bytes returned in first message. Expected 1 returned %d", len(messages[0]))
	}

	if len(messages[1]) != 3 {
		t.Errorf("unexpected amount of bytes returned in second message. Expected 3 returned %d", len(messages[1]))
	}
}

func BenchmarkConsumption(b *testing.B) {
	ms := []byte("message")
	bm := NewBenchmarkMarshal(ms)
	bb := bytes.NewBuffer(nil)

	for i := 0; i < b.N; i++ {
		bb.Write(join(ms, DefaultMessageDelimiter))
	}

	consumer := NewConsumer(bb, bm)
	defer consumer.Close()
	go consumer.Consume()

	subscription, _ := consumer.Subscribe(nil)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		<-subscription
	}
}
