package connectrpc

import (
	"net/http"
	"net/url"
	"time"

	"github.com/project-kgo/kc/pkg/registry"
)

type discoveryTransport struct {
	service  string
	resolver registry.Resolver
	base     http.RoundTripper
}

func (t *discoveryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	resolution, err := t.resolver.Resolve(request.Context(), t.service)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	outcome := registry.OutcomeFailure
	defer func() {
		latency := time.Since(started)
		if latency <= 0 {
			latency = time.Nanosecond
		}
		_ = resolution.Report(registry.Result{Outcome: outcome, Latency: latency})
	}()
	endpoint, err := url.Parse(resolution.Endpoint())
	if err != nil {
		return nil, err
	}
	clone := request.Clone(request.Context())
	clone.URL.Scheme = endpoint.Scheme
	clone.URL.Host = endpoint.Host
	clone.Host = endpoint.Host
	response, err := t.base.RoundTrip(clone)
	if err == nil && response != nil {
		outcome = registry.OutcomeSuccess
	}
	return response, err
}
