package nats

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ErrAckTimeout is an error when timeout was exceeded while waiting for an acknowledgment from server.
var ErrAckTimeout = errors.New("waiting for ack exceeded timeout")

// Nats holds a connection to nats broker/cluster and jetstream.
type Nats struct {
	conn        *nats.Conn
	stream      jetstream.JetStream
	consumers   []jetstream.Consumer
	router      *router
	logsEnabled bool
	tracer      trace.Tracer
}

// New is a constructor for [Nats].
func New(cfg Config, opts ...NatsOpt) (*Nats, error) {
	conn, err := nats.Connect(cfg.URL())
	if err != nil {
		return nil, fmt.Errorf("connect to nats: %w", err)
	}

	n := &Nats{
		conn:   conn,
		router: nil,
	}

	for _, opt := range opts {
		opt(n)
	}

	return n, nil
}

// Close releases captured resources for nats conn and stream.
func (n *Nats) Close() {
	_ = n.conn.Drain()
}

func (n *Nats) trace(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if n.tracer == nil {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := n.tracer.Start(ctx, spanName, trace.WithAttributes(attrs...))
	return ctx, span
}

func getMessageID(ctx context.Context, headers map[string]string) string {
	if headers["messageID"] != "" {
		return headers["messageID"]
	}

	messageID, ok := ctx.Value(MessageIDKey).(string)
	if !ok || messageID == "" {
		return uuid.NewString()
	}

	return messageID
}
