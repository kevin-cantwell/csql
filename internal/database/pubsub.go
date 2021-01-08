package database

import (
	"context"
	"math"
	"path/filepath"
	"sync"
	"time"
)

type Broker struct {
	mu          sync.Mutex
	subscribers []*subscriber
}

func (b *Broker) Publish(ctx context.Context, name string, tuple Tuple) int {
	event := Event{
		Stream: name,
		TS:     time.Now().UnixNano(),
		Tuple:  tuple,
	}

	b.mu.Lock()
	subs := b.subscribers
	b.mu.Unlock()

	var sent int
	for _, sub := range subs {
		if matches, _ := filepath.Match(sub.pattern, name); !matches {
			continue
		}
		select {
		case <-ctx.Done():
			return sent
		case sub.buf <- event:
			sent++
		case <-sub.closed:
		}
	}
	return sent
}

func (b *Broker) Subscribe(ctx context.Context, pattern string) <-chan Event {
	newSub := &subscriber{
		pattern: pattern,
		buf:     make(chan Event, math.MaxInt32), // big fucker
		closed:  make(chan struct{}),
	}
	results := make(chan Event)
	go func() {
		defer func() {
			b.mu.Lock()
			defer b.mu.Unlock()

			close(newSub.closed)
			close(results)

			for i, sub := range b.subscribers {
				if newSub == sub {
					b.subscribers = append(b.subscribers[:i], b.subscribers[i+1:]...)
					return
				}
			}
		}()
		for row := range newSub.buf {
			select {
			case <-ctx.Done():
				return
			case results <- row: // TODO: This may block on slow clients. Detect and handle.
			}
		}
	}()
	return results
}

type Event struct {
	Stream string `json:"stream,omitempty"`
	TS     int64  `json:"ts"`
	Tuple  Tuple  `json:"tuple"`
	Err    error  `json:"error,omitempty"`
}

type subscriber struct {
	pattern string
	buf     chan Event
	closed  chan struct{}
}

// func merge(ctx context.Context, streams []<-chan Tuple) <-chan Tuple {
// 	results := make(chan Tuple)
// 	for _, stream := range streams {
// 		stream := stream
// 		go func() {
// 			defer close(results)
// 			for row := range stream {
// 				select {
// 				case <-ctx.Done():
// 					return
// 				case results <- row: // TODO: this can block on slow clients
// 				}
// 			}
// 		}()
// 	}
// 	return results
// }

// func (b *Broker) subscribeAll(ctx context.Context, names []string) <-chan Tuple {
// 	var subs []<-chan Tuple
// 	for _, name := range names {
// 		subs = append(subs, b.Subscribe(ctx, name))
// 	}
// 	return merge(ctx, subs)
// }
