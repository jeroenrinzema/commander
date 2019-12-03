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

	bb := bytes.NewBuffer(append(chunk, DefaultMessageDelimiter))

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

	messages, remaining := consumer.SplitChunk([]byte{1, DefaultMessageDelimiter, 3, 4, 5, DefaultMessageDelimiter})

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
