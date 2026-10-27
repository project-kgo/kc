package mq

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Consumption 描述一次 Consume 调用，供 ConsumeAll 批量订阅。
type Consumption struct {
	Topic   string
	Group   string
	Handler Handler
	Options []SubscribeOption
}

// Subscriptions 是一组需要一起停止的订阅。
type Subscriptions []Subscription

// ConsumeAll 按顺序订阅全部 consumptions；任意一个失败时停止已经成功的订阅并返回错误，
// 保证调用方拿到的要么是全部订阅，要么一个都没有。
func ConsumeAll(ctx context.Context, subscriber Subscriber, consumptions ...Consumption) (Subscriptions, error) {
	if subscriber == nil {
		return nil, fmt.Errorf("%w: subscriber is nil", ErrInvalidArgument)
	}
	subscriptions := make(Subscriptions, 0, len(consumptions))
	for _, consumption := range consumptions {
		subscription, err := subscriber.Consume(ctx, consumption.Topic, consumption.Group, consumption.Handler, consumption.Options...)
		if err != nil {
			err = fmt.Errorf("consume topic %q group %q: %w", consumption.Topic, consumption.Group, err)
			// 刚启动的订阅可能已经有 Handler 在执行，回滚同样走优雅停止；ctx 已结束时直接取消。
			return nil, errors.Join(err, subscriptions.Stop(ctx))
		}
		subscriptions = append(subscriptions, subscription)
	}
	return subscriptions, nil
}

// Stop 并发停止全部订阅，等待每个订阅的 Stop 返回后合并错误。语义同 Subscription.Stop。
func (subscriptions Subscriptions) Stop(ctx context.Context) error {
	errs := make([]error, len(subscriptions))
	var wg sync.WaitGroup
	for index, subscription := range subscriptions {
		wg.Go(func() {
			errs[index] = subscription.Stop(ctx)
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}
