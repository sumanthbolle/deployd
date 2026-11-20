// Package telemetry sets up structured logs, Prometheus metrics and
// OpenTelemetry tracing.
package telemetry

import (
	"context"
	"log/slog"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Tracer is the package-wide tracer. Spans are dropped unless tracing is on.
var Tracer = otel.Tracer("github.com/sumanthbolle/deployd")

// Registry holds every metric deployd exposes on /metrics.
var Registry = prometheus.NewRegistry()

var (
	ReconcileTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "deployd_reconcile_total",
		Help: "Reconcile passes by result (converged, progressing, error).",
	}, []string{"result"})
	ReconcileSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "deployd_reconcile_duration_seconds",
		Help:    "Duration of one reconcile pass.",
		Buckets: prometheus.DefBuckets,
	})
	RolloutsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "deployd_rollouts_finished_total",
		Help: "Finished rollouts by final state.",
	}, []string{"state"})
	QueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "deployd_queue_depth",
		Help: "Jobs not yet done.",
	})
	RequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "deployd_api_requests_total",
		Help: "API requests by procedure and code.",
	}, []string{"procedure", "code"})
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		ReconcileTotal, ReconcileSeconds, RolloutsTotal, QueueDepth, RequestsTotal,
	)
}

// NewLogger returns a JSON logger at the given level ("debug", "info", ...).
func NewLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

// SetupTracing exports spans over OTLP/HTTP when OTEL_EXPORTER_OTLP_ENDPOINT
// is set (for example to a Jaeger or Grafana Tempo collector). Otherwise
// tracing stays a no-op. The returned function flushes spans on shutdown.
func SetupTracing(ctx context.Context, version string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res := resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceName("deployd"), semconv.ServiceVersion(version))
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
