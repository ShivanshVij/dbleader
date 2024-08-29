// SPDX-License-Identifier: Apache-2.0

package broadcast

import "sync"

type _subscriber[T any] struct {
	ch  chan<- T
	_ch <-chan T
}

type Broadcast[T any] struct {
	subscribers []*_subscriber[T]
	mu          sync.RWMutex
}

func New[T any]() *Broadcast[T] {
	return new(Broadcast[T])
}

func (b *Broadcast[T]) Subscribe() <-chan T {
	ch := make(chan T, 1)
	subscriber := &_subscriber[T]{ch, ch}
	b.mu.Lock()
	b.subscribers = append(b.subscribers, subscriber)
	b.mu.Unlock()
	return subscriber._ch
}

func (b *Broadcast[T]) Unsubscribe(ch <-chan T) {
	b.mu.Lock()
	for i, subscriber := range b.subscribers {
		if subscriber._ch == ch {
			b.subscribers = append(b.subscribers[:i], b.subscribers[i+1:]...)
			close(subscriber.ch)
			break
		}
	}
	b.mu.Unlock()
}

func (b *Broadcast[T]) Publish(msg T) {
	b.mu.RLock()
	for _, subscriber := range b.subscribers {
		select {
		case subscriber.ch <- msg:
		default:
		}
	}
	b.mu.RUnlock()
}
