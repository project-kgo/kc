package connectrpc

import (
	"context"
	"errors"
	"net/http"
	"runtime/debug"

	"connectrpc.com/connect"
)

type PanicEvent struct {
	Procedure string
	Value     any
	Stack     []byte
}

type PanicHandler func(context.Context, PanicEvent)

func Recovery(handler PanicHandler) connect.HandlerOption {
	return connect.WithRecover(func(ctx context.Context, spec connect.Spec, _ http.Header, recovered any) error {
		if handler != nil {
			handler(ctx, PanicEvent{Procedure: spec.Procedure, Value: recovered, Stack: debug.Stack()})
		}
		return connect.NewError(connect.CodeInternal, errors.New("internal server error"))
	})
}
