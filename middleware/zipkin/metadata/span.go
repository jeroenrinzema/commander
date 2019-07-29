package metadata

import (
	"context"

	zipkin "github.com/openzipkin/zipkin-go"
)

// NewSpanConsumeContext creates a new context with Span attached. If used
// NewSpanConsumeContext will overwrite any previously-appended metadata.
func NewSpanConsumeContext(ctx context.Context, span zipkin.Span) context.Context {
	return context.WithValue(ctx, CtxSpanConsume, span)
}

// SpanConsumeFromContext returns the Span in ctx if it exists.
// The returned Span should not be modified. Writing to it may cause races. Modification should be made to copies of the returned Header.
func SpanConsumeFromContext(ctx context.Context) (span zipkin.Span, ok bool) {
	span, ok = ctx.Value(CtxSpanConsume).(zipkin.Span)
	return
}

// NewSpanProduceContext creates a new context with Span attached. If used
// NewSpanProduceContext will overwrite any previously-appended metadata.
func NewSpanProduceContext(ctx context.Context, span zipkin.Span) context.Context {
	return context.WithValue(ctx, CtxSpanProduce, span)
}

// SpanProduceFromContext returns the Span in ctx if it exists.
// The returned Span should not be modified. Writing to it may cause races. Modification should be made to copies of the returned Header.
func SpanProduceFromContext(ctx context.Context) (span zipkin.Span, ok bool) {
	span, ok = ctx.Value(CtxSpanProduce).(zipkin.Span)
	return
}
