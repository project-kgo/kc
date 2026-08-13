// Package mq 定义与具体消息中间件无关的发布、订阅契约。
package mq

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalidArgument 表示 MQ 操作收到空上下文、主题、消费组、消息或处理函数。
	ErrInvalidArgument = errors.New("mq: invalid argument")
	// ErrClosed 表示 MQ 客户端已经关闭或正在关闭。
	ErrClosed = errors.New("mq: closed")
)

// Message 描述一条与具体消息队列实现无关的消息。
// ID 由消费端填充且应被视为不透明值；发布时不会使用该字段。
// 消费端收到的 Key、Body 和 Headers value 仅保证在 Handler 调用期间有效，
// 应视为只读；如需修改、异步使用或在 Handler 返回后持有，调用方必须自行复制。
type Message struct {
	ID        string
	Key       []byte
	Body      []byte
	Headers   map[string][]byte
	Timestamp time.Time
}

// Handler 处理一条订阅消息。仅当 Handler 返回 nil 且处理上下文仍有效时，消息才会被确认。
// Handler 必须并发安全、能够幂等处理重复消息，并及时响应 context 取消。
type Handler func(context.Context, *Message) error

// Publisher 提供同步发布消息的能力。
type Publisher interface {
	Publish(ctx context.Context, topic string, message *Message) error
}

// Subscriber 提供按主题和消费组异步订阅消息的能力。
// Subscribe 在订阅后台任务启动后返回，后台消费会一直持续到 ctx 取消或 MQ 关闭。
// options 仅影响本次订阅；同一客户端上的不同订阅可以使用不同参数。
type Subscriber interface {
	Subscribe(ctx context.Context, topic, group string, handler Handler, options ...SubscribeOption) error
}

// MQ 组合消息发布、订阅和资源释放能力。
// Close 会停止拉取新消息，并等待已经开始的 Handler 及其确认完成。
type MQ interface {
	Publisher
	Subscriber
	Close() error
}
