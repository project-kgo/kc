package registry_test

import (
	"context"
	"net/http"
	"time"

	"github.com/project-kgo/kc/pkg/registry"
)

// This helper shows the intended Connect-compatible h2c request lifecycle:
// resolve immediately before the request, then report transport health.
func send(ctx context.Context, resolver registry.Resolver, request *http.Request) (*http.Response, error) {
	resolution, err := resolver.Resolve(ctx, "users.v1")
	if err != nil {
		return nil, err
	}
	request.URL.Scheme = "http"
	endpoint := resolution.Endpoint()
	request.URL.Host = endpoint[len("http://"):]

	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{Protocols: protocols}
	started := time.Now()
	outcome := registry.OutcomeFailure
	defer func() {
		_ = resolution.Report(registry.Result{Outcome: outcome, Latency: time.Since(started)})
	}()
	response, err := transport.RoundTrip(request)
	if err == nil {
		outcome = registry.OutcomeSuccess
	}
	return response, err
}
