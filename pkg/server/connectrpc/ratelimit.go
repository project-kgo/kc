package connectrpc

import (
	"context"
	"errors"
	"sync"

	"connectrpc.com/connect"
	"golang.org/x/time/rate"
)

type RateLimitConfig struct {
	Rate  rate.Limit
	Burst int
}

func RateLimit(config RateLimitConfig) (connect.Interceptor, error) {
	if config.Rate <= 0 || config.Burst <= 0 {
		return nil, errors.New("connectrpc server: rate and burst must be positive")
	}
	return &rateLimitInterceptor{rate: config.Rate, burst: config.Burst, limiters: make(map[string]*rate.Limiter)}, nil
}

type rateLimitInterceptor struct {
	rate     rate.Limit
	burst    int
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
}

func (i *rateLimitInterceptor) allow(procedure string) bool {
	i.mu.Lock()
	limiter := i.limiters[procedure]
	if limiter == nil {
		limiter = rate.NewLimiter(i.rate, i.burst)
		i.limiters[procedure] = limiter
	}
	i.mu.Unlock()
	return limiter.Allow()
}

func (i *rateLimitInterceptor) rejection() error {
	return connect.NewError(connect.CodeResourceExhausted, errors.New("rate limit exceeded"))
}

func (i *rateLimitInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		if !i.allow(request.Spec().Procedure) {
			return nil, i.rejection()
		}
		return next(ctx, request)
	}
}

func (i *rateLimitInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if !i.allow(conn.Spec().Procedure) {
			return i.rejection()
		}
		return next(ctx, conn)
	}
}

func (i *rateLimitInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}
