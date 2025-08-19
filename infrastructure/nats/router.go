package nats

import (
	"context"
	"fmt"
	"sync"
)

// StreamEventHandler is a handler for incoming messages from stream.
type StreamEventHandler func(ctx context.Context, m *Message) error

// router maps incoming messages to corresponding handlers.
type router struct {
	handlers sync.Map
}

// processStreamMessage processes message with handler found by subject.
func (r *router) processStreamMessage(ctx context.Context, subj string, m *Message) error {
	h, ok := r.handlers.Load(subj)
	if !ok {
		return nil
	}

	handler, ok := h.(StreamEventHandler)
	if !ok {
		panic("not an event handler")
	}

	err := handler(ctx, m)
	if err != nil {
		return fmt.Errorf("handler returned an error: %w", err)
	}

	return nil
}
