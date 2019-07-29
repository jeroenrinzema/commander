package metadata

// Key context key type
type Key string

// Zipkin context keys
const (
	CtxSpanConsume = Key("SpanConsume")
	CtxSpanProduce = Key("SpanProduce")
)

// Header keys
const (
	HeaderTraceID      = "x-trace-id"
	HeaderSpanID       = "x-span-id"
	HeaderParentSpanID = "x-parent-span-id"
	HeaderSampled      = "x-trace-sampled"
)
