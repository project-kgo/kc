// Package mqruntime 实现各 MQ 客户端共享的生命周期：开闭状态、进行中的同步操作、活跃订阅，
// 以及 Consume 返回的订阅句柄。具体中间件只负责拉取、处理和确认消息。
package mqruntime

import (
	"context"
	"fmt"
	"strings"
	"sync"

	coremq "github.com/project-kgo/kc/pkg/mq"
)

// ValidateSubscribe 校验 Subscribe 与 Consume 共用的入参，ctx 已结束时返回 ctx 的错误。
func ValidateSubscribe(ctx context.Context, topic, group string, handler coremq.Handler) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", coremq.ErrInvalidArgument)
	}
	if strings.TrimSpace(topic) == "" {
		return fmt.Errorf("%w: topic is empty", coremq.ErrInvalidArgument)
	}
	if strings.TrimSpace(group) == "" {
		return fmt.Errorf("%w: consumer group is empty", coremq.ErrInvalidArgument)
	}
	if handler == nil {
		return fmt.Errorf("%w: handler is nil", coremq.ErrInvalidArgument)
	}
	return ctx.Err()
}

type state uint8

const (
	stateOpen state = iota
	stateClosing
	stateClosed
)

// Lifecycle 跟踪 MQ 客户端的开闭状态、进行中的同步操作和活跃订阅。必须用 NewLifecycle 创建。
type Lifecycle struct {
	mu            sync.Mutex
	state         state
	subscriptions map[*Subscription]struct{}
	operations    sync.WaitGroup
	closeDone     chan struct{}
	closeErr      error
}

func NewLifecycle() *Lifecycle {
	return &Lifecycle{subscriptions: make(map[*Subscription]struct{}), closeDone: make(chan struct{})}
}

// Begin 登记一次同步操作（发布、建立订阅等）；客户端已开始关闭时返回 false。
func (l *Lifecycle) Begin() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != stateOpen {
		return false
	}
	l.operations.Add(1)
	return true
}

// End 结束 Begin 登记的操作。
func (l *Lifecycle) End() {
	l.operations.Done()
}

// Closed 报告客户端是否已开始关闭。
func (l *Lifecycle) Closed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state != stateOpen
}

// Active 返回仍在运行的订阅数量。
func (l *Lifecycle) Active() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.subscriptions)
}

// NewSubscription 创建订阅句柄。parent 结束时立即停止拉取并取消 Handler：
// Subscribe 传入调用方的 ctx，Consume 传入 context.WithoutCancel 后的 ctx，只能由句柄停止。
// 句柄在 Track 之前不会被 Close 感知，创建失败时调用方用 Abort 回滚。
func (l *Lifecycle) NewSubscription(parent context.Context) *Subscription {
	pollCtx, stopPoll := context.WithCancel(parent)
	workCtx, abort := context.WithCancelCause(parent)
	return &Subscription{
		lifecycle: l,
		pollCtx:   pollCtx, stopPoll: stopPoll,
		workCtx: workCtx, abort: abort,
		done: make(chan struct{}),
	}
}

// Track 把订阅登记为活跃订阅；客户端已开始关闭时返回 false，调用方应回滚已创建的资源。
func (l *Lifecycle) Track(subscription *Subscription) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != stateOpen {
		return false
	}
	l.subscriptions[subscription] = struct{}{}
	return true
}

func (l *Lifecycle) untrack(subscription *Subscription) {
	l.mu.Lock()
	delete(l.subscriptions, subscription)
	l.mu.Unlock()
}

// Close 停止全部订阅的拉取，等待进行中的操作和订阅退出后调用 release 释放底层客户端。
// ctx 先结束时以 ErrClosed 取消仍在执行的 Handler 并继续等待，release 仍会执行。
// 并发或重复调用会等待第一次关闭完成并返回同一结果。
func (l *Lifecycle) Close(ctx context.Context, release func() error) error {
	l.mu.Lock()
	if l.state != stateOpen {
		done := l.closeDone
		l.mu.Unlock()
		select {
		case <-done:
			l.mu.Lock()
			defer l.mu.Unlock()
			return l.closeErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	l.state = stateClosing
	subscriptions := make([]*Subscription, 0, len(l.subscriptions))
	for subscription := range l.subscriptions {
		subscription.stopPoll()
		subscriptions = append(subscriptions, subscription)
	}
	l.mu.Unlock()

	// state 已经变为 closing，不会再有新的 operations.Add 与 Wait 并发发生。
	stopped := make(chan struct{})
	go func() {
		l.operations.Wait()
		for _, subscription := range subscriptions {
			<-subscription.done
		}
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
		for _, subscription := range subscriptions {
			subscription.abort(coremq.ErrClosed)
		}
		<-stopped
	}
	var err error
	if release != nil {
		err = release()
	}

	l.mu.Lock()
	l.closeErr = err
	l.state = stateClosed
	close(l.closeDone)
	l.mu.Unlock()
	return err
}

var _ coremq.Subscription = (*Subscription)(nil)

// Subscription 是一次订阅的生命周期句柄：PollContext 结束表示停止拉取、不再启动新的 Handler，
// WorkContext 是 Handler 及其确认的上下文，只在强制停止或 Subscribe 的 ctx 结束时取消。
// 具体实现在全部后台任务退出后调用 Finish。
type Subscription struct {
	lifecycle *Lifecycle
	pollCtx   context.Context
	stopPoll  context.CancelFunc
	workCtx   context.Context
	abort     context.CancelCauseFunc
	done      chan struct{}
	finish    sync.Once
}

// PollContext 返回拉取循环使用的上下文。
func (s *Subscription) PollContext() context.Context { return s.pollCtx }

// WorkContext 返回 Handler 和确认使用的上下文。
func (s *Subscription) WorkContext() context.Context { return s.workCtx }

// Abort 立即停止拉取并以 cause 取消 Handler，用于建立订阅失败时回滚。
func (s *Subscription) Abort(cause error) {
	s.stopPoll()
	s.abort(cause)
}

// Finish 由具体实现在订阅的全部后台任务退出后调用，释放上下文并通知等待方。
func (s *Subscription) Finish() {
	s.finish.Do(func() {
		s.Abort(context.Canceled)
		s.lifecycle.untrack(s)
		close(s.done)
	})
}

// Done 实现 [coremq.Subscription]。
func (s *Subscription) Done() <-chan struct{} { return s.done }

// Stop 实现 [coremq.Subscription]。
func (s *Subscription) Stop(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", coremq.ErrInvalidArgument)
	}
	s.stopPoll()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
	}
	select {
	case <-s.done:
		return nil
	default:
	}
	s.abort(coremq.ErrClosed)
	<-s.done
	return ctx.Err()
}
