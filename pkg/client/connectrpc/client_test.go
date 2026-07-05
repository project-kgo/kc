package connectrpc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/project-kgo/kc/pkg/registry"
)

type fakeResolver struct {
	resolution *fakeResolution
	err        error
}

func (r *fakeResolver) Resolve(context.Context, string) (registry.Resolution, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.resolution, nil
}
func (*fakeResolver) Close() error { return nil }

type fakeResolution struct {
	endpoint string
	mu       sync.Mutex
	results  []registry.Result
}

func (r *fakeResolution) Endpoint() string            { return r.endpoint }
func (r *fakeResolution) Instance() registry.Instance { return registry.Instance{Endpoint: r.endpoint} }
func (r *fakeResolution) Report(result registry.Result) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, result)
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestClientRewritesEndpointAndReportsSuccess(t *testing.T) {
	resolution := &fakeResolution{endpoint: "http://127.0.0.1:9090"}
	resolver := &fakeResolver{resolution: resolution}
	var got *http.Request
	c, err := NewDiscovery("users.v1", resolver, WithRoundTripper(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got = r
		return &http.Response{StatusCode: http.StatusInternalServerError, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("x")), Request: r}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	original, _ := http.NewRequest(http.MethodPost, c.BaseURL()+"/users.v1.User/Get", nil)
	response, err := c.HTTPClient().Do(original)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if got.URL.Host != "127.0.0.1:9090" || got.URL.Path != "/users.v1.User/Get" {
		t.Fatalf("unexpected target %s", got.URL)
	}
	if original.URL.Host == got.URL.Host {
		t.Fatal("original request was mutated")
	}
	if len(resolution.results) != 1 || resolution.results[0].Outcome != registry.OutcomeSuccess {
		t.Fatalf("unexpected reports: %#v", resolution.results)
	}
}

func TestClientReportsTransportFailure(t *testing.T) {
	resolution := &fakeResolution{endpoint: "http://127.0.0.1:9090"}
	want := errors.New("dial failed")
	c, err := NewDiscovery("users.v1", &fakeResolver{resolution: resolution}, WithRoundTripper(roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, want })))
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPost, c.BaseURL()+"/rpc", nil)
	_, err = c.HTTPClient().Do(request)
	if !errors.Is(err, want) {
		t.Fatalf("got %v", err)
	}
	if len(resolution.results) != 1 || resolution.results[0].Outcome != registry.OutcomeFailure || resolution.results[0].Latency <= 0 {
		t.Fatalf("unexpected reports: %#v", resolution.results)
	}
}

func TestDefaultProtocolsEnableHTTP1AndH2C(t *testing.T) {
	c, err := NewDiscovery("users.v1", &fakeResolver{resolution: &fakeResolution{}}, WithHTTPTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := c.transport.base.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected transport %T", c.transport.base)
	}
	if !transport.Protocols.HTTP1() || !transport.Protocols.UnencryptedHTTP2() {
		t.Fatalf("unexpected protocols: %s", transport.Protocols)
	}
}

func TestDefaultTransportUsesInternalPreset(t *testing.T) {
	c, err := NewDiscovery("users.v1", &fakeResolver{resolution: &fakeResolution{}})
	if err != nil {
		t.Fatal(err)
	}
	assertTransportPreset(t, c.transport.base, TransportInternal)
}

func TestTransportPresets(t *testing.T) {
	tests := []struct {
		preset                TransportPreset
		proxy                 bool
		dialTimeout           time.Duration
		tlsHandshakeTimeout   time.Duration
		maxIdleConns          int
		maxIdleConnsPerHost   int
		idleConnTimeout       time.Duration
		responseHeaderTimeout time.Duration
		http2                 bool
		h2c                   bool
	}{
		{TransportInternal, false, 3 * time.Second, 5 * time.Second, 256, 64, 90 * time.Second, 30 * time.Second, false, true},
		{TransportPublicTLS, true, 5 * time.Second, 10 * time.Second, 100, 20, 90 * time.Second, 30 * time.Second, true, false},
		{TransportStreaming, false, 3 * time.Second, 5 * time.Second, 256, 64, 5 * time.Minute, 0, false, true},
	}
	for _, test := range tests {
		t.Run(test.preset.String(), func(t *testing.T) {
			c, err := NewDiscovery("users.v1", &fakeResolver{resolution: &fakeResolution{}}, WithTransportPreset(test.preset))
			if err != nil {
				t.Fatal(err)
			}
			transport := c.transport.base.(*http.Transport)
			if (transport.Proxy != nil) != test.proxy || transport.TLSHandshakeTimeout != test.tlsHandshakeTimeout ||
				transport.MaxIdleConns != test.maxIdleConns || transport.MaxIdleConnsPerHost != test.maxIdleConnsPerHost ||
				transport.IdleConnTimeout != test.idleConnTimeout || transport.ResponseHeaderTimeout != test.responseHeaderTimeout ||
				transport.ExpectContinueTimeout != time.Second || transport.MaxResponseHeaderBytes != 1<<20 {
				t.Fatalf("transport does not match %s preset: %#v", test.preset, transport)
			}
			if !transport.Protocols.HTTP1() || transport.Protocols.HTTP2() != test.http2 || transport.Protocols.UnencryptedHTTP2() != test.h2c {
				t.Fatalf("unexpected protocols: %s", transport.Protocols)
			}
			presetConfig, err := test.preset.config()
			if err != nil {
				t.Fatal(err)
			}
			if presetConfig.dialer.Timeout != test.dialTimeout || presetConfig.dialer.KeepAlive != 30*time.Second {
				t.Fatalf("unexpected dialer: %#v", presetConfig.dialer)
			}
		})
	}
}

func TestTransportPresetRejectsInvalidValue(t *testing.T) {
	if _, err := NewDiscovery("users.v1", &fakeResolver{resolution: &fakeResolution{}}, WithTransportPreset(TransportPreset(255))); err == nil {
		t.Fatal("expected invalid preset error")
	}
}

func TestRoundTripperOverridesTransportPreset(t *testing.T) {
	custom := roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("unused") })
	c, err := NewDiscovery(
		"users.v1",
		&fakeResolver{resolution: &fakeResolution{}},
		WithTransportPreset(TransportPublicTLS),
		WithRoundTripper(custom),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.transport.base.(roundTripFunc); !ok {
		t.Fatalf("unexpected round tripper %T", c.transport.base)
	}
}

func TestHTTPTimeoutAppliesToAllTransportPresets(t *testing.T) {
	for _, preset := range []TransportPreset{TransportInternal, TransportPublicTLS, TransportStreaming} {
		t.Run(preset.String(), func(t *testing.T) {
			c, err := NewDiscovery(
				"users.v1",
				&fakeResolver{resolution: &fakeResolution{}},
				WithTransportPreset(preset),
				WithHTTPTimeout(17*time.Second),
			)
			if err != nil {
				t.Fatal(err)
			}
			if c.httpClient.Timeout != 17*time.Second {
				t.Fatalf("timeout=%v", c.httpClient.Timeout)
			}
		})
	}
}

func TestProtocolsOverridePresetHTTP1AndH2C(t *testing.T) {
	c, err := NewDiscovery(
		"users.v1",
		&fakeResolver{resolution: &fakeResolution{}},
		WithTransportPreset(TransportPublicTLS),
		WithProtocols(false, true),
	)
	if err != nil {
		t.Fatal(err)
	}
	transport := c.transport.base.(*http.Transport)
	if transport.Protocols.HTTP1() || !transport.Protocols.HTTP2() || !transport.Protocols.UnencryptedHTTP2() {
		t.Fatalf("unexpected protocols: %s", transport.Protocols)
	}
}

func assertTransportPreset(t *testing.T, roundTripper http.RoundTripper, preset TransportPreset) {
	t.Helper()
	transport, ok := roundTripper.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected transport %T", roundTripper)
	}
	want, err := preset.config()
	if err != nil {
		t.Fatal(err)
	}
	if (transport.Proxy != nil) != (want.proxy != nil) {
		t.Fatalf("proxy configured=%v, want %v", transport.Proxy != nil, want.proxy != nil)
	}
	if transport.MaxIdleConns != want.maxIdleConns || transport.MaxIdleConnsPerHost != want.maxIdleConnsPerHost ||
		transport.IdleConnTimeout != want.idleConnTimeout || transport.ResponseHeaderTimeout != want.responseHeaderTimeout ||
		transport.TLSHandshakeTimeout != want.tlsHandshakeTimeout || transport.ExpectContinueTimeout != want.expectContinueTimeout ||
		transport.MaxResponseHeaderBytes != want.maxResponseHeaderBytes {
		t.Fatalf("transport does not match %s preset: %#v", preset, transport)
	}
	if transport.Protocols.HTTP1() != want.http1 || transport.Protocols.HTTP2() != want.http2 ||
		transport.Protocols.UnencryptedHTTP2() != want.h2c {
		t.Fatalf("protocols=%s", transport.Protocols)
	}
	if want.dialer.Timeout <= 0 || want.dialer.KeepAlive != 30*time.Second {
		t.Fatalf("unexpected dialer: %#v", want.dialer)
	}
}

func TestNewRemainsDiscoveryAlias(t *testing.T) {
	c, err := New("users.v1", &fakeResolver{resolution: &fakeResolution{}}, WithHTTPTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if c.transport == nil {
		t.Fatal("expected discovery transport")
	}
}

func TestDirectClientUsesConfiguredBaseURL(t *testing.T) {
	var got *http.Request
	c, err := NewDirect("http://127.0.0.1:8080/api", WithRoundTripper(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got = r
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok")), Request: r}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPost, c.BaseURL()+"/users.v1.User/Get", nil)
	response, err := c.HTTPClient().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if got.URL.String() != "http://127.0.0.1:8080/api/users.v1.User/Get" {
		t.Fatalf("unexpected target %s", got.URL.String())
	}
	if c.transport != nil {
		t.Fatal("direct client should not use discovery transport")
	}
}

func TestDirectClientRejectsInvalidBaseURL(t *testing.T) {
	cases := []string{
		"",
		"users.v1",
		"/relative",
		"http://127.0.0.1:8080?x=1",
		"http://127.0.0.1:8080#frag",
	}
	for _, baseURL := range cases {
		t.Run(baseURL, func(t *testing.T) {
			if _, err := NewDirect(baseURL); err == nil {
				t.Fatalf("expected error for %q", baseURL)
			}
		})
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
