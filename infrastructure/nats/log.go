package nats

import (
	"context"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

// loggerCtx wraps a logger into context.
func loggerCtx(ctx context.Context) context.Context {
	spanCtx := trace.SpanContextFromContext(ctx)
	l := log.With().Str("trace_id", spanCtx.TraceID().String()).Logger()
	return l.WithContext(ctx)
}
