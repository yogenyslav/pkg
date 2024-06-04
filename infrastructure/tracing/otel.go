// Package tracing provides a wrapper around OpenTelemetry tracing.
package tracing

import (
	"context"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// MustSetupOTel setups OpenTelemetry tracing with the given configuration.
func MustSetupOTel(cfg *Config, name string) {
	exporter := MustNewExporter(context.Background(), cfg.URL())

	provider := MustNewTraceProvider(exporter, name)
	otel.SetTracerProvider(provider)
}

// MustNewExporter creates a new OTLP trace exporter or panics if failed.
func MustNewExporter(ctx context.Context, endpoint string) sdktrace.SpanExporter {
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithInsecure(), otlptracehttp.WithEndpoint(endpoint))
	if err != nil {
		log.Panic().Err(err).Msg("failed to create OTLP trace exporter")
	}
	return exporter
}

// MustNewTraceProvider creates a new OTel trace provider or panics if failed.
func MustNewTraceProvider(exp sdktrace.SpanExporter, name string) *sdktrace.TracerProvider {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(name),
		),
	)
	if err != nil {
		log.Panic().Err(err).Msg("failed to create tracing resource")
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)
}
