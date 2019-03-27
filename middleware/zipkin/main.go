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
	CtxSpanKey CtxKey = "ZipkinCtxSpan"
)

// Zipkin Kafka message header keys
const (
	HeaderTraceIDKey      = "HeaderTraceIDKey"
	HeaderSpanIDKey       = "HeaderSpanIDKey"
	HeaderParentSpanIDKey = "HeaderParentSpanIDKey"
	HeaderSampledKey      = "HeaderSampledKey"
	HeaderFlagsKey        = "HeaderFlagsKey"
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
}

// Close closes the Zipkin reporter
func (middleware *Zipkin) Close() error {
	return middleware.Reporter.Close()
}

// StartSpan starts a new span and stores it in the message context
func (middleware *Zipkin) StartSpan(message *commander.Message) error {
	options := []zipkin.SpanOption{zipkin.Kind(model.Consumer)}

	ctx, err := ExtractContextFromMessage(message)
	if err == nil {
		options = append(options, zipkin.Parent(ctx))
	}

	action := message.Headers[commander.ActionHeader]
	span := middleware.Tracer.StartSpan(action, options...)
	message.Ctx = context.WithValue(message.Ctx, CtxSpanKey, span)
	return nil
}

// FinishSpan finishes the stored span in the message context
func (middleware *Zipkin) FinishSpan(message *commander.Message) error {
	if message.Ctx == nil {
		return errors.New("message context is not set")
	}

	intrf := message.Ctx.Value(CtxSpanKey)
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

	return sc, nil
}

// ConstructMessageHeaders construct message headers for a commander header
func ConstructMessageHeaders(span model.SpanContext) map[string]string {
	headers := map[string]string{
		HeaderTraceIDKey: span.TraceID.String(),
		HeaderSpanIDKey:  span.ID.String(),
	}

	return headers
}
