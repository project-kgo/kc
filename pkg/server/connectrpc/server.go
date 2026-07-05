// Package connectrpc provides lifecycle management for Connect, gRPC, and
// gRPC-Web handlers served over HTTP/1.1 and unencrypted HTTP/2.
package connectrpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/project-kgo/kc/pkg/registry"
)

type Server struct {
	mu              sync.Mutex
	running         bool
	paths           map[string]struct{}
	mux             *http.ServeMux
	httpServer      *http.Server
	address         string
	shutdownTimeout time.Duration
	handlerOptions  []connect.HandlerOption
	registrar       registry.Registrar
	registration    RegistrationConfig
	listener        net.Listener
}

// HandlerFactory constructs a generated Connect handler with the supplied
// handler options.
type HandlerFactory func(...connect.HandlerOption) (string, http.Handler)

func New(options ...Option) (*Server, error) {
	cfg := config{address: ":8080", http1: true, h2c: true, readHeaderTimeout: 10 * time.Second, idleTimeout: 2 * time.Minute, shutdownTimeout: 30 * time.Second}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("connectrpc server: nil option")
		}
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(cfg.http1)
	protocols.SetUnencryptedHTTP2(cfg.h2c)
	mux := http.NewServeMux()
	httpServer := &http.Server{Handler: mux, Protocols: protocols, ReadHeaderTimeout: cfg.readHeaderTimeout, IdleTimeout: cfg.idleTimeout}
	return &Server{paths: make(map[string]struct{}), mux: mux, httpServer: httpServer, address: cfg.address, shutdownTimeout: cfg.shutdownTimeout, handlerOptions: cfg.handlerOptions, registrar: cfg.registrar, registration: cfg.registration, listener: cfg.listener}, nil
}

// HandlerOptions returns global handler options followed by handler-local options.
func (s *Server) HandlerOptions(local ...connect.HandlerOption) []connect.HandlerOption {
	options := make([]connect.HandlerOption, 0, len(s.handlerOptions)+len(local))
	options = append(options, s.handlerOptions...)
	return append(options, local...)
}

// HandleFactory registers a generated Connect handler after applying the
// server's global handler options followed by handler-local options.
func (s *Server) HandleFactory(factory HandlerFactory, local ...connect.HandlerOption) error {
	if factory == nil {
		return errors.New("connectrpc server: nil handler factory")
	}
	path, handler := factory(s.HandlerOptions(local...)...)
	return s.Handle(path, handler)
}

// Handle registers an already-constructed HTTP handler. It is a low-level
// compatibility API and does not apply the server's global handler options.
func (s *Server) Handle(path string, handler http.Handler) error {
	if strings.TrimSpace(path) == "" || handler == nil {
		return errors.New("connectrpc server: invalid handler")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return errors.New("connectrpc server: cannot add handler after Run")
	}
	if _, exists := s.paths[path]; exists {
		return errors.New("connectrpc server: duplicate handler path")
	}
	if err := registerHandler(s.mux, path, handler); err != nil {
		return err
	}
	s.paths[path] = struct{}{}
	return nil
}

func registerHandler(mux *http.ServeMux, path string, handler http.Handler) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("connectrpc server: register handler %q: %v", path, recovered)
		}
	}()
	mux.Handle(path, handler)
	return nil
}

func (s *Server) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("connectrpc server: Run may only be called once")
	}
	s.running = true
	s.mu.Unlock()

	listener := s.listener
	var err error
	if listener == nil {
		listener, err = net.Listen("tcp", s.address)
		if err != nil {
			return err
		}
	}
	var registration registry.Registration
	if s.registrar != nil {
		cfg := s.registration
		instance := registry.Instance{Service: cfg.Service, ID: cfg.ID, Endpoint: cfg.Endpoint, Metadata: cfg.Metadata}
		registration, err = s.registrar.Register(ctx, instance, cfg.TTL)
		if err != nil {
			_ = listener.Close()
			return err
		}
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.httpServer.Serve(listener) }()

	var cause error
	if registration == nil {
		select {
		case <-ctx.Done():
		case err = <-serveErr:
			cause = normalizeServeError(err)
		}
	} else {
		select {
		case <-ctx.Done():
		case err = <-serveErr:
			cause = normalizeServeError(err)
		case <-registration.Done():
			cause = registration.Err()
			if cause == nil {
				cause = errors.New("connectrpc server: registration ended")
			}
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	if registration != nil {
		cause = errors.Join(cause, registration.Close(shutdownCtx))
	}
	cause = errors.Join(cause, s.httpServer.Shutdown(shutdownCtx))
	return cause
}

func normalizeServeError(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
