package connectrpc

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"time"
)

// TransportPreset selects a built-in HTTP transport configuration.
type TransportPreset uint8

const (
	// TransportInternal is tuned for ordinary internal RPC over HTTP/1.1 or h2c.
	TransportInternal TransportPreset = iota
	// TransportPublicTLS is tuned for public TLS endpoints and enables HTTP/2.
	TransportPublicTLS
	// TransportStreaming is tuned for long-lived internal streaming RPCs.
	TransportStreaming
)

func (p TransportPreset) String() string {
	switch p {
	case TransportInternal:
		return "internal"
	case TransportPublicTLS:
		return "public-tls"
	case TransportStreaming:
		return "streaming"
	default:
		return "unknown"
	}
}

type transportPresetConfig struct {
	proxy                  func(*http.Request) (*url.URL, error)
	dialer                 net.Dialer
	tlsHandshakeTimeout    time.Duration
	maxIdleConns           int
	maxIdleConnsPerHost    int
	idleConnTimeout        time.Duration
	responseHeaderTimeout  time.Duration
	expectContinueTimeout  time.Duration
	maxResponseHeaderBytes int64
	http1                  bool
	http2                  bool
	h2c                    bool
}

func (p TransportPreset) config() (transportPresetConfig, error) {
	const responseHeaderLimit = 1 << 20
	base := transportPresetConfig{
		dialer:                 net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second},
		tlsHandshakeTimeout:    5 * time.Second,
		maxIdleConns:           256,
		maxIdleConnsPerHost:    64,
		idleConnTimeout:        90 * time.Second,
		responseHeaderTimeout:  30 * time.Second,
		expectContinueTimeout:  time.Second,
		maxResponseHeaderBytes: responseHeaderLimit,
		http1:                  true,
		h2c:                    true,
	}
	switch p {
	case TransportInternal:
		return base, nil
	case TransportPublicTLS:
		base.proxy = http.ProxyFromEnvironment
		base.dialer.Timeout = 5 * time.Second
		base.tlsHandshakeTimeout = 10 * time.Second
		base.maxIdleConns = 100
		base.maxIdleConnsPerHost = 20
		base.http2 = true
		base.h2c = false
		return base, nil
	case TransportStreaming:
		base.idleConnTimeout = 5 * time.Minute
		base.responseHeaderTimeout = 0
		return base, nil
	default:
		return transportPresetConfig{}, errors.New("connectrpc client: invalid transport preset")
	}
}

func (c transportPresetConfig) transport() *http.Transport {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(c.http1)
	protocols.SetHTTP2(c.http2)
	protocols.SetUnencryptedHTTP2(c.h2c)
	return &http.Transport{
		Proxy:                  c.proxy,
		DialContext:            c.dialer.DialContext,
		TLSHandshakeTimeout:    c.tlsHandshakeTimeout,
		MaxIdleConns:           c.maxIdleConns,
		MaxIdleConnsPerHost:    c.maxIdleConnsPerHost,
		IdleConnTimeout:        c.idleConnTimeout,
		ResponseHeaderTimeout:  c.responseHeaderTimeout,
		ExpectContinueTimeout:  c.expectContinueTimeout,
		MaxResponseHeaderBytes: c.maxResponseHeaderBytes,
		Protocols:              protocols,
	}
}
