package consumer

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jeroenrinzema/commander/types"

	"github.com/Shopify/sarama"
)

var (
	topic = types.Topic{
		Name: "testing",
	}
)

func attemptDeadlock(partitions, consumers int, b *testing.B) {
	ctx := context.Background()

	breaker := false
	wg := sync.WaitGroup{}

	client := &Client{
		channels: make(map[string]*Channel),
	}

	template := sarama.ConsumerMessage{
		Key:     []byte{},
		Value:   []byte{},
		Topic:   topic.Name,
		Offset:  0,
		Headers: []*sarama.RecordHeader{},
	}

	for offset := 0; offset < partitions; offset++ {
		wg.Add(1)

		go func(partition int) {
			for {
				if breaker {
					break
				}

				date := time.Now().Add(100 * time.Millisecond)
				deadline, cancel := context.WithDeadline(ctx, date)
				defer cancel()

				read := make(chan bool, 0)
				go func(partition int) {
					message := template
					message.Partition = int32(partition)
					client.Claim(&message)
					close(read)
				}(partition)

				select {
				case <-deadline.Done():
					panic("deadlock detected")
				case <-read:
				}
			}

			wg.Done()
		}(offset)
	}

	go func() {
		expected := consumers
		for {
			if breaker {
				break
			}

			if expected <= 0 {
				break
			}

			wg.Add(1)

			// Setup a consumer with a random deadline
			go func() {
				min := time.Now()
				// max := min.Add(100 * time.Millisecond)
				delta := min.Unix() + 1

				sec := rand.Int63n(delta) + min.Unix()
				deadline, cancel := context.WithDeadline(ctx, time.Unix(sec, 0))
				defer cancel()

				sink, mark, _ := client.Subscribe(topic)
				go func() {
					for {
						<-sink
						mark <- nil
					}
				}()

				<-deadline.Done()

				client.Unsubscribe(sink)
				expected++
				wg.Done()
			}()

			expected--
		}
	}()

	<-time.After(10 * time.Second)
	breaker = true
	wg.Wait()
}

func BenchmarkAttemptDeadlock50Part10Cons(b *testing.B) { attemptDeadlock(50, 10, b) }
