package tracing

import (
	"context"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

func MustSetupOTel(endpoint, name string) {
	exporter := MustNewExporter(context.Background(), endpoint)

	provider := MustNewTraceProvider(exporter, name)
	otel.SetTracerProvider(provider)
}

func MustNewExporter(ctx context.Context, endpoint string) trace.SpanExporter {
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithInsecure(), otlptracehttp.WithEndpoint(endpoint))
	if err != nil {
		log.Panic().Err(err)
	}
	return exporter
}

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
