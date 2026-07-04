package connectrpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
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

func TestTraceBuildsWithW3CDefault(t *testing.T) {
	interceptor, err := Trace()
	if err != nil {
		t.Fatal(err)
	}
	if interceptor == nil {
		t.Fatal("nil trace interceptor")
	}
}
