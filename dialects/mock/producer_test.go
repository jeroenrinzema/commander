package mock

import (
	"context"
	"testing"

	"github.com/jeroenrinzema/commander/internal/types"
)

// TestProducerProduction tests if able to produce messages
func TestProducerProduction(t *testing.T) {
	dialect := NewDialect()
	topic := types.NewTopic("mock", dialect, types.EventMessage, types.DefaultMode)
	message := types.Message{
		Topic: topic,
		Ctx:   context.Background(),
	}

	err := dialect.Producer().Publish(&message)
	if err != nil {
		t.Fatal(err)
	}
}
