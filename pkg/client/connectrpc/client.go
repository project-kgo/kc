// Package connectrpc provides Connect clients backed by service discovery or direct URLs.
package connectrpc

import (
	"errors"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/project-kgo/kc/pkg/registry"
)

type Option func(*config) error

type config struct {
	roundTripper http.RoundTripper
	timeout      time.Duration
	http1        bool
	h2c          bool
	interceptors []connect.Interceptor
}

func WithRoundTripper(roundTripper http.RoundTripper) Option {
	return func(c *config) error {
		if roundTripper == nil {
			return errors.New("connectrpc client: nil round tripper")
		}
		c.roundTripper = roundTripper
		return nil
	}
}

func WithHTTPTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		if timeout < 0 {
			return errors.New("connectrpc client: negative HTTP timeout")
		}
		c.timeout = timeout
		return nil
	}
}

func WithProtocols(http1, unencryptedHTTP2 bool) Option {
	return func(c *config) error {
		if !http1 && !unencryptedHTTP2 {
			return errors.New("connectrpc client: at least one HTTP protocol is required")
		}
		c.http1, c.h2c = http1, unencryptedHTTP2
		return nil
	}
}

func WithInterceptors(interceptors ...connect.Interceptor) Option {
	return func(c *config) error {
		for _, interceptor := range interceptors {
			if interceptor == nil {
				return errors.New("connectrpc client: nil interceptor")
			}
		}
		c.interceptors = append(c.interceptors, interceptors...)
		return nil
	}
}

type Client struct {
	httpClient *http.Client
	baseURL    string
	options    []connect.ClientOption
	transport  *discoveryTransport
}

func New(service string, resolver registry.Resolver, options ...Option) (*Client, error) {
	return NewDiscovery(service, resolver, options...)
}

func NewDiscovery(service string, resolver registry.Resolver, options ...Option) (*Client, error) {
	if strings.TrimSpace(service) == "" || strings.ContainsAny(service, `/\\`) {
		return nil, errors.New("connectrpc client: invalid service")
	}
	if resolver == nil {
		return nil, errors.New("connectrpc client: nil resolver")
	}
	cfg, base, clientOptions, err := buildConfig(options...)
	if err != nil {
		return nil, err
	}
	transport := &discoveryTransport{service: service, resolver: resolver, base: base}
	return &Client{
		httpClient: &http.Client{Transport: transport, Timeout: cfg.timeout},
		baseURL:    "http://connectrpc.invalid",
		options:    clientOptions,
		transport:  transport,
	}, nil
}

func NewDirect(baseURL string, options ...Option) (*Client, error) {
	endpoint, err := parseBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	cfg, base, clientOptions, err := buildConfig(options...)
	if err != nil {
		return nil, err
	}
	return &Client{
		httpClient: &http.Client{Transport: base, Timeout: cfg.timeout},
		baseURL:    endpoint,
		options:    clientOptions,
	}, nil
}

func buildConfig(options ...Option) (config, http.RoundTripper, []connect.ClientOption, error) {
	cfg := config{http1: true, h2c: true}
	for _, option := range options {
		if option == nil {
			return config{}, nil, nil, errors.New("connectrpc client: nil option")
		}
		if err := option(&cfg); err != nil {
			return config{}, nil, nil, err
		}
	}
	base := cfg.roundTripper
	if base == nil {
		protocols := new(http.Protocols)
		protocols.SetHTTP1(cfg.http1)
		protocols.SetUnencryptedHTTP2(cfg.h2c)
		base = &http.Transport{Protocols: protocols}
	}
	clientOptions := make([]connect.ClientOption, 0, 1)
	if len(cfg.interceptors) > 0 {
		clientOptions = append(clientOptions, connect.WithInterceptors(cfg.interceptors...))
	}
	return cfg, base, clientOptions, nil
}

func parseBaseURL(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", errors.New("connectrpc client: invalid base URL")
	}
	endpoint, err := url.Parse(raw)
	if err != nil {
		return "", errors.New("connectrpc client: invalid base URL")
	}
	if endpoint.Scheme == "" || endpoint.Host == "" {
		return "", errors.New("connectrpc client: invalid base URL")
	}
	if endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return "", errors.New("connectrpc client: base URL must not include query or fragment")
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return "", errors.New("connectrpc client: unsupported base URL scheme")
	}
	cleaned := *endpoint
	cleaned.Path = path.Clean("/" + strings.TrimSpace(endpoint.Path))
	if cleaned.Path == "/" {
		cleaned.Path = ""
	}
	return strings.TrimSuffix(cleaned.String(), "/"), nil
}

func (c *Client) HTTPClient() *http.Client { return c.httpClient }
func (c *Client) BaseURL() string          { return c.baseURL }
func (c *Client) Options() []connect.ClientOption {
	return append([]connect.ClientOption(nil), c.options...)
}
func (c *Client) CloseIdleConnections() { c.httpClient.CloseIdleConnections() }
