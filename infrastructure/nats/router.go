package nats

import (
	"context"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// StreamEventHandler is a handler for incoming jetstream messages.
type StreamEventHandler func(ctx context.Context, m jetstream.Msg) error

// router maps incoming messages to corresponding handlers.
type router struct {
	streamHandlers sync.Map
}

// processStreamMessage processes message with handler found by subject.
func (r *router) processStreamMessage(ctx context.Context, m jetstream.Msg) error {
	h, ok := r.streamHandlers.Load(m.Subject())
	if !ok {
		return nil
	}

	handler, ok := h.(StreamEventHandler)
	if !ok {
		panic("not a stream event handler")
	}

	err := handler(ctx, m)
	if err != nil {
		return fmt.Errorf("handler returned an error: %v", err)
	}

	return nil
}
