package metadata

import (
	"context"
	"strconv"

	"github.com/jeroenrinzema/commander"
	
	"github.com/jeroenrinzema/commander/internal/types"
	"github.com/openzipkin/zipkin-go/model"
)

// ExtractContextFromMessageHeaders attempts to extracts the span context from the message headers.
func ExtractContextFromMessageHeaders(message *commander.Message) (span model.SpanContext, ok bool) {
	var headers types.Header
	headers, ok = types.HeaderFromContext(message.Ctx)
	if !ok {
		return span, ok
	}

	var err error
	var id uint64
	var trace model.TraceID
	var parent uint64
	var sampled bool

	id, err = strconv.ParseUint((headers[HeaderSpanID]).String(), 16, 64)
	if err != nil {
		return span, false
	}

	trace, err = model.TraceIDFromHex((headers[HeaderTraceID]).String())
	if err != nil {
		return span, false
	}

	sampled = (headers[HeaderSampled]).String() == "1"

	span.ID = model.ID(id)
	span.TraceID = trace
	span.Sampled = &sampled

	parent, err = strconv.ParseUint((headers[HeaderParentSpanID]).String(), 16, 64)
	if err == nil {
		typed := model.ID(parent)
		span.ParentID = &typed
	}

	return span, true
}

// AppendMessageHeaders construct and appends message span headers
func AppendMessageHeaders(ctx context.Context, span model.SpanContext) context.Context {
	sampled := "0"
	if span.Sampled != nil {
		if *span.Sampled {
			sampled = "1"
		}
	}

	var parent string
	if span.ParentID != nil {
		parent = span.ParentID.String()
	}

	headers := types.Header{
		HeaderTraceID:      []string{span.TraceID.String()},
		HeaderSpanID:       []string{span.ID.String()},
		HeaderParentSpanID: []string{parent},
		HeaderSampled:      []string{sampled},
	}

	return types.AppendToHeaderContext(ctx, headers)
}
