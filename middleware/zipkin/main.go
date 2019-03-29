package zipkin

import (
	"context"
	"errors"
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
	CtxSpanKeyConsume CtxKey = "ZipkinCtxSpanConsume"
	CtxSpanKeyProduce CtxKey = "ZipkinCtxSpanProduce"
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
	subscribe(commander.BeforeActionConsumption, middleware.BeforeConsume)
	subscribe(commander.AfterActionConsumption, middleware.AfterConsume)
	subscribe(commander.BeforePublish, middleware.BeforePublish)
	subscribe(commander.AfterPublish, middleware.AfterPublish)
}

// Close closes the Zipkin reporter
func (middleware *Zipkin) Close() error {
	return middleware.Reporter.Close()
}

// BeforeConsume starts a new span and stores it in the message context
func (middleware *Zipkin) BeforeConsume(event *commander.MiddlewareEvent) error {
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
	name := "commander.consume." + action
	span := middleware.Tracer.StartSpan(name, options...)
	message.Ctx = context.WithValue(message.Ctx, CtxSpanKeyConsume, span)

	span.Tag(ActionTag, message.Headers[commander.ActionHeader])
	span.Tag(StatusTag, message.Headers[commander.StatusHeader])
	span.Tag(VersionTag, message.Headers[commander.VersionHeader])

	return nil
}

// AfterConsume finishes the stored span in the message context
func (middleware *Zipkin) AfterConsume(event *commander.MiddlewareEvent) error {
	commander.Logger.Println("finishing span")

	intrf := event.Ctx.Value(CtxSpanKeyConsume)
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

// BeforePublish prepares the given message span headers
func (middleware *Zipkin) BeforePublish(event *commander.MiddlewareEvent) error {
	commander.Logger.Println("injecting span into message")
	message, ok := event.Value.(*commander.Message)
	if !ok {
		return errors.New("value is not a *message")
	}

	intrf := event.Ctx.Value(CtxSpanKeyConsume)
	if intrf == nil {
		return errors.New("message context contains no tracing span")
	}

	msp, ok := intrf.(zipkin.Span)
	if !ok {
		return errors.New("interface is not a Zipkin span")
	}

	name := "commander.produce"
	span := middleware.Tracer.StartSpan(name, zipkin.Kind(model.Consumer), zipkin.Parent(msp.Context()))
	message.Ctx = context.WithValue(message.Ctx, CtxSpanKeyProduce, span)

	headers := ConstructMessageHeaders(msp.Context())
	for k, v := range headers {
		message.Headers[k] = v
	}

	return nil
}

// AfterPublish closes the producing span
func (middleware *Zipkin) AfterPublish(event *commander.MiddlewareEvent) error {
	commander.Logger.Println("finishing span")

	intrf := event.Ctx.Value(CtxSpanKeyProduce)
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
