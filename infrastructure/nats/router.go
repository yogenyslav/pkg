package nats

import (
	"context"
	"fmt"
	"sync"
)

// EventHandler is a handler for incoming messages.
type EventHandler func(ctx context.Context, m *Message) error

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

	handler, ok := h.(EventHandler)
	if !ok {
		panic("not an event handler")
	}

	err := handler(ctx, m)
	if err != nil {
		return fmt.Errorf("handler returned an error: %w", err)
	}

	return nil
}
