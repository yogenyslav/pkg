package nats

import (
	"go.opentelemetry.io/otel/trace"
)

// NatsOpt is an alias for nats options.
type NatsOpt func(*Nats)

// WithTracer enables tracing for nats calls.
func WithTracer(tracer trace.Tracer) NatsOpt {
	return func(n *Nats) {
		n.tracer = tracer
	}
}

// WithJetStream enables jetstream support.
func WithJetStream() NatsOpt {
	return func(n *Nats) {
		n.JetStream()
	}
}

func WithLogs(enabled bool) NatsOpt {
	return func(n *Nats) {
		n.logsEnabled = enabled
	}
}
