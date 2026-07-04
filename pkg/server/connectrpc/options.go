package connectrpc

import (
	"errors"
	"net"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/project-kgo/kc/pkg/registry"
)

type Option func(*config) error

type config struct {
	address           string
	http1             bool
	h2c               bool
	readHeaderTimeout time.Duration
	idleTimeout       time.Duration
	shutdownTimeout   time.Duration
	handlerOptions    []connect.HandlerOption
	registrar         registry.Registrar
	registration      RegistrationConfig
	listener          net.Listener
}

// WithListener uses an existing listener. Server takes ownership and closes it.
func WithListener(listener net.Listener) Option {
	return func(c *config) error {
		if listener == nil {
			return errors.New("connectrpc server: nil listener")
		}
		c.listener = listener
		return nil
	}
}

type RegistrationConfig struct {
	Service  string
	ID       string
	Endpoint string
	Metadata map[string]string
	TTL      time.Duration
}

func WithAddress(address string) Option {
	return func(c *config) error {
		if strings.TrimSpace(address) == "" {
			return errors.New("connectrpc server: empty address")
		}
		c.address = address
		return nil
	}
}

func WithProtocols(http1, unencryptedHTTP2 bool) Option {
	return func(c *config) error {
		if !http1 && !unencryptedHTTP2 {
			return errors.New("connectrpc server: at least one HTTP protocol is required")
		}
		c.http1, c.h2c = http1, unencryptedHTTP2
		return nil
	}
}

func WithInterceptors(interceptors ...connect.Interceptor) Option {
	return func(c *config) error {
		for _, interceptor := range interceptors {
			if interceptor == nil {
				return errors.New("connectrpc server: nil interceptor")
			}
		}
		if len(interceptors) > 0 {
			c.handlerOptions = append(c.handlerOptions, connect.WithInterceptors(interceptors...))
		}
		return nil
	}
}

func WithHandlerOptions(options ...connect.HandlerOption) Option {
	return func(c *config) error {
		for _, option := range options {
			if option == nil {
				return errors.New("connectrpc server: nil handler option")
			}
		}
		c.handlerOptions = append(c.handlerOptions, options...)
		return nil
	}
}

func WithTimeouts(readHeader, idle, shutdown time.Duration) Option {
	return func(c *config) error {
		if readHeader <= 0 || idle <= 0 || shutdown <= 0 {
			return errors.New("connectrpc server: timeouts must be positive")
		}
		c.readHeaderTimeout, c.idleTimeout, c.shutdownTimeout = readHeader, idle, shutdown
		return nil
	}
}

func WithRegistry(registrar registry.Registrar, registration RegistrationConfig) Option {
	return func(c *config) error {
		if registrar == nil {
			return errors.New("connectrpc server: nil registrar")
		}
		instance := registry.Instance{Service: registration.Service, ID: registration.ID, Endpoint: registration.Endpoint, Metadata: registration.Metadata}
		if err := instance.Validate(); err != nil {
			return err
		}
		if registration.TTL <= 0 {
			return errors.New("connectrpc server: registry TTL must be positive")
		}
		registration.Metadata = instance.Clone().Metadata
		c.registrar, c.registration = registrar, registration
		return nil
	}
}
