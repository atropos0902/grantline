// Package obs exports metrics and traces.
package obs

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"

	v1 "grantline/gen/grantline/v1"
)

// Traces sends spans to an OTLP endpoint. With no endpoint, spans cost nothing.
func Traces(ctx context.Context, endpoint, replication string) (stop func(), err error) {
	if endpoint == "" {
		return func() {}, nil
	}
	x, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
	if err != nil {
		return nil, err
	}
	p := trace.NewTracerProvider(trace.WithBatcher(x), trace.WithSampler(trace.TraceIDRatioBased(0.1)),
		trace.WithResource(resource.NewSchemaless(attribute.String("service.name", "grantline"), attribute.String("grantline.replication", replication))))
	otel.SetTracerProvider(p)
	return func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = p.Shutdown(c)
	}, nil
}

// Collector turns a worker's status into metrics when they are scraped.
type Collector struct {
	Status func(context.Context) *v1.Status
}

type metric struct {
	desc *prometheus.Desc
	kind prometheus.ValueType
	get  func(*v1.Status) float64
}

func m(name, help string, kind prometheus.ValueType, get func(*v1.Status) float64) metric {
	return metric{prometheus.NewDesc("grantline_"+name, help, []string{"replication"}, nil), kind, get}
}

func b(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

var metrics = []metric{
	m("up", "1 when the engine is running", prometheus.GaugeValue, func(s *v1.Status) float64 { return b(s.Failure == "" && s.Phase != 0) }),
	m("ready", "1 when the copy can be read", prometheus.GaugeValue, func(s *v1.Status) float64 { return b(s.Phase == v1.Phase_PHASE_READY) }),
	m("paused", "1 while paused or waiting for a schema change", prometheus.GaugeValue, func(s *v1.Status) float64 { return b(s.Paused || s.WaitingStatement != "") }),
	m("lag_seconds", "age of the newest applied heartbeat", prometheus.GaugeValue, func(s *v1.Status) float64 { return s.Lag.AsDuration().Seconds() }),
	m("retained_log_bytes", "log the source keeps for the slot", prometheus.GaugeValue, func(s *v1.Status) float64 { return float64(s.RetainedLogBytes) }),
	m("epoch", "ownership epoch", prometheus.GaugeValue, func(s *v1.Status) float64 { return float64(s.Epoch) }),
	m("chunks", "chunks planned", prometheus.GaugeValue, func(s *v1.Status) float64 { return float64(s.Chunks) }),
	m("chunks_landed", "chunks landed", prometheus.GaugeValue, func(s *v1.Status) float64 { return float64(s.ChunksLanded) }),
	m("changes_total", "changes applied", prometheus.CounterValue, func(s *v1.Status) float64 { return float64(s.Changes) }),
	m("rows_copied_total", "chunk rows landed", prometheus.CounterValue, func(s *v1.Status) float64 { return float64(s.RowsCopied) }),
	m("rows_refused_total", "chunk rows older than the stream, left out", prometheus.CounterValue, func(s *v1.Status) float64 { return float64(s.RowsRefused) }),
	m("keys_reread_total", "keys read again", prometheus.CounterValue, func(s *v1.Status) float64 { return float64(s.KeysReread) }),
	m("spilled_transactions_total", "transactions moved to disk", prometheus.CounterValue, func(s *v1.Status) float64 { return float64(s.SpilledTransactions) }),
	m("commits_total", "groups committed", prometheus.CounterValue, func(s *v1.Status) float64 { return float64(s.Commits) }),
	m("yields_total", "times a lane gave up its locks", prometheus.CounterValue, func(s *v1.Status) float64 { return float64(s.Yields) }),
}

// Describe implements prometheus.Collector.
func (c Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range metrics {
		ch <- m.desc
	}
}

// Collect implements prometheus.Collector.
func (c Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st := c.Status(ctx)
	for _, m := range metrics {
		ch <- prometheus.MustNewConstMetric(m.desc, m.kind, m.get(st), st.Replication)
	}
}
