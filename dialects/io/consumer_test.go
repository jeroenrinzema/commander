package io

import "testing"

func TestSplittingBufferedChunk(t *testing.T) {
	delimiter := byte(2)

	consumer := NewConsumer(nil, nil)
	consumer.MessageDelimiter = delimiter

	messages, remaining := consumer.SplitChunk([]byte{1, delimiter, 3, 4, 5, delimiter})
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
