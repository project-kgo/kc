package connectrpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestRecoveryReturnsInternalAndCallsHandler(t *testing.T) {
	var event PanicEvent
	handler := connect.NewUnaryHandler("/test.Service/Panic", func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
		panic("secret panic")
	}, Recovery(func(_ context.Context, got PanicEvent) { event = got }))
	request := httptest.NewRequest(http.MethodPost, "/test.Service/Panic", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status %d: %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "secret panic") {
		t.Fatal("panic leaked to response")
	}
	if event.Procedure != "/test.Service/Panic" || event.Value != "secret panic" || len(event.Stack) == 0 {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestRateLimitIsPerProcedure(t *testing.T) {
	interceptor, err := RateLimit(RateLimitConfig{Rate: 1, Burst: 1})
	if err != nil {
		t.Fatal(err)
	}
	next := func(context.Context, connect.AnyRequest) (connect.AnyResponse, error) { return nil, nil }
	wrapped := interceptor.WrapUnary(next)
	// Procedure is populated by Connect during a real call; validate limiter isolation through its internal key path.
	if !interceptor.(*rateLimitInterceptor).allow("/svc/A") {
		t.Fatal("first A rejected")
	}
	if interceptor.(*rateLimitInterceptor).allow("/svc/A") {
		t.Fatal("second A allowed")
	}
	if !interceptor.(*rateLimitInterceptor).allow("/svc/B") {
		t.Fatal("first B rejected")
	}
	_, _ = wrapped, errors.New("")
}

func TestRateLimitRejectsInvalidConfig(t *testing.T) {
	if _, err := RateLimit(RateLimitConfig{}); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestTraceUsesGlobalPropagatorToExtractTraceAndBaggage(t *testing.T) {
	originalPropagator := otel.GetTextMapPropagator()
	originalTracerProvider := otel.GetTracerProvider()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	otel.SetTracerProvider(tracerProvider)
	t.Cleanup(func() {
		_ = tracerProvider.Shutdown(context.Background())
		otel.SetTracerProvider(originalTracerProvider)
		otel.SetTextMapPropagator(originalPropagator)
	})

	interceptor, err := Trace()
	if err != nil {
		t.Fatal(err)
	}
	var gotTraceID trace.TraceID
	var gotSpanID trace.SpanID
	var gotTenant string
	handler := connect.NewUnaryHandler(
		"/test.Service/Trace",
		func(ctx context.Context, _ *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
			spanContext := trace.SpanContextFromContext(ctx)
			gotTraceID = spanContext.TraceID()
			gotSpanID = spanContext.SpanID()
			gotTenant = baggage.FromContext(ctx).Member("tenant").Value()
			return connect.NewResponse(&emptypb.Empty{}), nil
		},
		connect.WithInterceptors(interceptor),
	)
	request := httptest.NewRequest(http.MethodPost, "/test.Service/Trace", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	request.Header.Set("Baggage", "tenant=acme")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body.String())
	}
	if gotTraceID.String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace=%s span=%s", gotTraceID, gotSpanID)
	}
	if gotSpanID.String() == "00f067aa0ba902b7" || !gotSpanID.IsValid() {
		t.Fatalf("server did not create a child span: %s", gotSpanID)
	}
	if gotTenant != "acme" {
		t.Fatalf("tenant=%q", gotTenant)
	}
}
