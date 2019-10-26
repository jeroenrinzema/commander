package metadata

import (
	"context"
)

// Kafka message metadata
type Kafka struct {
	Offset    int64
	Partition int32
}

// NewKafkaContext creates a new context with Retries attached. If used
// NewKafkaContext will overwrite any previously-appended metadata.
func NewKafkaContext(ctx context.Context, info Kafka) context.Context {
	return context.WithValue(ctx, CtxKafka, info)
}

// KafkaFromContext returns the Kafka in ctx if it exists.
// The returned Kafka should not be modified. Writing to it may cause races. Modification should be made to copies of the returned Kafka.
func KafkaFromContext(ctx context.Context) (info Kafka, ok bool) {
	info, ok = ctx.Value(CtxKafka).(Kafka)
	return
}
