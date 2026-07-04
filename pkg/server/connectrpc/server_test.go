package connectrpc

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/project-kgo/kc/pkg/registry"
	"google.golang.org/protobuf/types/known/emptypb"
)

type countingInterceptor struct{ calls *atomic.Int32 }

func (i countingInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		i.calls.Add(1)
		return next(ctx, request)
	}
}
func (i countingInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}
func (i countingInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func emptyHandler(options ...connect.HandlerOption) (string, http.Handler) {
	return "/test.Service/", connect.NewUnaryHandler("/test.Service/Empty", func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
		return connect.NewResponse(&emptypb.Empty{}), nil
	}, options...)
}

func TestServerDefaultsAndGlobalLocalInterceptors(t *testing.T) {
	var global, local atomic.Int32
	server, err := New(WithInterceptors(countingInterceptor{&global}))
	if err != nil {
		t.Fatal(err)
	}
	if !server.httpServer.Protocols.HTTP1() || !server.httpServer.Protocols.UnencryptedHTTP2() {
		t.Fatalf("unexpected protocols: %s", server.httpServer.Protocols)
	}
	if err := server.Handle(emptyHandler(server.HandlerOptions(connect.WithInterceptors(countingInterceptor{&local}))...)); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/test.Service/Empty", nil)
	request.Header.Set("Content-Type", "application/proto")
	response := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body.String())
	}
	if global.Load() != 1 || local.Load() != 1 {
		t.Fatalf("global=%d local=%d", global.Load(), local.Load())
	}
}

type fakeRegistration struct {
	closed atomic.Bool
	done   chan struct{}
}

func (r *fakeRegistration) Done() <-chan struct{} { return r.done }
func (*fakeRegistration) Err() error              { return nil }
func (r *fakeRegistration) Close(context.Context) error {
	if r.closed.CompareAndSwap(false, true) {
		close(r.done)
	}
	return nil
}

type fakeRegistrar struct {
	registration *fakeRegistration
	instance     registry.Instance
	ttl          time.Duration
}

func (r *fakeRegistrar) Register(_ context.Context, instance registry.Instance, ttl time.Duration) (registry.Registration, error) {
	r.instance, r.ttl = instance, ttl
	return r.registration, nil
}

type fakeListener struct{ closed chan struct{} }

func (l *fakeListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, errors.New("closed")
}
func (l *fakeListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}
func (*fakeListener) Addr() net.Addr { return fakeAddr("test") }

type fakeAddr string

func (a fakeAddr) Network() string { return string(a) }
func (a fakeAddr) String() string  { return string(a) }

func TestServerRunRegistersAndClosesRegistration(t *testing.T) {
	registration := &fakeRegistration{done: make(chan struct{})}
	registrar := &fakeRegistrar{registration: registration}
	server, err := New(
		WithListener(&fakeListener{closed: make(chan struct{})}),
		WithRegistry(registrar, RegistrationConfig{Service: "users.v1", ID: "one", Endpoint: "http://127.0.0.1:8080", TTL: time.Second}),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !registration.closed.Load() {
		t.Fatal("registration was not closed")
	}
	if registrar.instance.Service != "users.v1" || registrar.ttl != time.Second {
		t.Fatalf("unexpected registration: %#v %v", registrar.instance, registrar.ttl)
	}
}

func TestServerRejectsInvalidRegistryConfig(t *testing.T) {
	_, err := New(WithRegistry(&fakeRegistrar{}, RegistrationConfig{Service: "users.v1"}))
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestHandleReturnsServeMuxConflict(t *testing.T) {
	server, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Handle("GET /items/{id}", http.NotFoundHandler()); err != nil {
		t.Fatal(err)
	}
	if err := server.Handle("GET /items/{name}", http.NotFoundHandler()); err == nil {
		t.Fatal("expected route conflict")
	}
}
