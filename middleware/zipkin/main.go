package zipkin

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jeroenrinzema/commander"
	zipkin "github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	reporter "github.com/openzipkin/zipkin-go/reporter"
	httprecorder "github.com/openzipkin/zipkin-go/reporter/http"
)

// CtxKey represents a Zipkin context key
type CtxKey string

// Available Zipkin context keys
const (
	CtxSpanKey CtxKey = "ZipkinCtxSpan"
)

// Zipkin Kafka message header keys
const (
	HeaderTraceIDKey      = "trace_id"
	HeaderSpanIDKey       = "span_id"
	HeaderParentSpanIDKey = "parent_span_id"
	HeaderSampledKey      = "trace_sampled"
)

// Zipkin span tags
const (
	ActionTag  = "commander.message.action"
	StatusTag  = "commander.message.status"
	VersionTag = "commander.message.version"
)

// New initializes a new Zipkin commander middleware reporter
func New(connectionstring string) (*Zipkin, error) {
	keyval := ParseConnectionstring(connectionstring)
	err := ValidateConnectionKeyVal(keyval)
	if err != nil {
		return nil, err
	}

	config, err := NewConfig(keyval)
	if err != nil {
		return nil, err
	}

	endpoint := &model.Endpoint{
		ServiceName: config.ServiceName,
	}

	reporter := httprecorder.NewReporter(config.ZipkinHost)
	tracer, err := zipkin.NewTracer(reporter, zipkin.WithLocalEndpoint(endpoint))
	if err != nil {
		return nil, err
	}

	middleware := &Zipkin{
		Reporter: reporter,
		Tracer:   tracer,
		Config:   config,
	}

	return middleware, nil
}

// Zipkin represents a Zipkin middleware instance
type Zipkin struct {
	Reporter reporter.Reporter
	Tracer   *zipkin.Tracer
	Config   Config
}

// Controller is a middleware controller that set's up the needed middleware
// event subscriptions.
func (middleware *Zipkin) Controller(subscribe commander.MiddlewareSubscribe) {
	subscribe(commander.BeforeConsumption, middleware.StartSpan)
	subscribe(commander.AfterConsumed, middleware.FinishSpan)
	subscribe(commander.BeforePublish, middleware.PrepareMessage)
}

// Close closes the Zipkin reporter
func (middleware *Zipkin) Close() error {
	return middleware.Reporter.Close()
}

// StartSpan starts a new span and stores it in the message context
func (middleware *Zipkin) StartSpan(event *commander.MiddlewareEvent) error {
	commander.Logger.Println("starting span")
	message, ok := event.Value.(*commander.Message)
	if !ok {
		return errors.New("value is not a *message")
	}

	options := []zipkin.SpanOption{zipkin.Kind(model.Consumer)}
	sc, err := ExtractContextFromMessage(message)
	if err == nil {
		options = append(options, zipkin.Parent(sc))
	}

	action := message.Headers[commander.ActionHeader]
	parent := message.Headers[commander.ParentHeader]
	sort := "message"

	if parent != "" {
		sort = "event"
	} else {
		sort = "command"
	}

	name := fmt.Sprintf("%s_%s", sort, action)
	span := middleware.Tracer.StartSpan(name, options...)
	message.Ctx = context.WithValue(message.Ctx, CtxSpanKey, span)

	span.Tag(ActionTag, message.Headers[commander.ActionHeader])
	span.Tag(StatusTag, message.Headers[commander.StatusHeader])
	span.Tag(VersionTag, message.Headers[commander.VersionHeader])

	return nil
}

// FinishSpan finishes the stored span in the message context
func (middleware *Zipkin) FinishSpan(event *commander.MiddlewareEvent) error {
	commander.Logger.Println("finishing span")

	intrf := event.Ctx.Value(CtxSpanKey)
	if intrf == nil {
		return errors.New("message context contains no tracing span")
	}

	span, ok := intrf.(zipkin.Span)
	if !ok {
		return errors.New("interface is not a Zipkin span")
	}

	span.Finish()
	return nil
}

// PrepareMessage prepares the given message span headers
func (middleware *Zipkin) PrepareMessage(event *commander.MiddlewareEvent) error {
	commander.Logger.Println("injecting span into message")
	message, ok := event.Value.(*commander.Message)
	if !ok {
		return errors.New("value is not a *message")
	}

	intrf := event.Ctx.Value(CtxSpanKey)
	if intrf == nil {
		return errors.New("message context contains no tracing span")
	}

	span, ok := intrf.(zipkin.Span)
	if !ok {
		return errors.New("interface is not a Zipkin span")
	}

	headers := ConstructMessageHeaders(span.Context())
	for k, v := range headers {
		message.Headers[k] = v
	}

	return nil
}

// ExtractContextFromMessage extracts the span context of a Commander message
func ExtractContextFromMessage(message *commander.Message) (model.SpanContext, error) {
	commander.Logger.Println("attempting span context extraction from message")
	sc := model.SpanContext{}

	id, err := model.TraceIDFromHex(message.Headers[HeaderTraceIDKey])
	if err != nil {
		return sc, err
	}

	sc.TraceID = id

	spanID, err := strconv.ParseUint(message.Headers[HeaderSpanIDKey], 16, 64)
	if err != nil {
		return sc, err
	}

	sc.ID = model.ID(spanID)
	parentID, err := strconv.ParseUint(message.Headers[HeaderParentSpanIDKey], 16, 64)
	if err == nil {
		id := model.ID(parentID)
		sc.ParentID = &id
	}

	if message.Headers[HeaderSampledKey] == "1" {
		b := true
		sc.Sampled = &b
	}

	return sc, nil
}

// ConstructMessageHeaders construct message headers for a commander header
func ConstructMessageHeaders(span model.SpanContext) map[string]string {
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

	headers := map[string]string{
		HeaderTraceIDKey:      span.TraceID.String(),
		HeaderSpanIDKey:       span.ID.String(),
		HeaderParentSpanIDKey: parent,
		HeaderSampledKey:      sampled,
	}

	return headers
}
