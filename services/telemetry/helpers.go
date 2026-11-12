package telemetry

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "bnpb"

func kv(pairs []any) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		key, ok := pairs[i].(string)
		if !ok {
			continue
		}
		switch v := pairs[i+1].(type) {
		case string:
			out = append(out, attribute.String(key, v))
		case int:
			out = append(out, attribute.Int(key, v))
		case int64:
			out = append(out, attribute.Int64(key, v))
		case bool:
			out = append(out, attribute.Bool(key, v))
		case float64:
			out = append(out, attribute.Float64(key, v))
		}
	}
	return out
}

func StartSpan(ctx context.Context, name string, attrs ...any) (context.Context, func(error)) {
	ctx, span := otel.Tracer(instrumentationName).Start(ctx, name, trace.WithAttributes(kv(attrs)...))
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func StartConsumerSpan(ctx context.Context, name string, headers map[string]any, attrs ...any) (context.Context, func(error)) {
	ctx = Extract(ctx, headers)
	ctx, span := otel.Tracer(instrumentationName).Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(kv(attrs)...))
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func StartProducerSpan(ctx context.Context, name string, headers map[string]any, attrs ...any) (context.Context, func(error)) {
	ctx, span := otel.Tracer(instrumentationName).Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(kv(attrs)...))
	Inject(ctx, headers)
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

var (
	instrumentsMu sync.Mutex
	counters      = map[string]metric.Int64Counter{}
)

func Counter(name, help string) func(ctx context.Context, n int64, attrs ...any) {
	instrumentsMu.Lock()
	defer instrumentsMu.Unlock()
	c, ok := counters[name]
	if !ok {
		var err error
		c, err = otel.Meter(instrumentationName).Int64Counter(name, metric.WithDescription(help))
		if err != nil {
			return func(context.Context, int64, ...any) {}
		}
		counters[name] = c
	}
	return func(ctx context.Context, n int64, attrs ...any) {
		c.Add(ctx, n, metric.WithAttributes(kv(attrs)...))
	}
}

type Sample struct {
	Value float64
	Attrs []any
}

func GaugeFunc(name, help string, fn func() []Sample) {
	g, err := otel.Meter(instrumentationName).Float64ObservableGauge(name, metric.WithDescription(help))
	if err != nil {
		return
	}
	_, _ = otel.Meter(instrumentationName).RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, s := range fn() {
			o.ObserveFloat64(g, s.Value, metric.WithAttributes(kv(s.Attrs)...))
		}
		return nil
	}, g)
}

var (
	histogramsMu sync.Mutex
	histograms   = map[string]metric.Float64Histogram{}
)

var LatencyBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 7.5, 10, 30, 60}

func Histogram(name, help string, boundaries ...float64) func(ctx context.Context, value float64, attrs ...any) {
	if len(boundaries) == 0 {
		boundaries = LatencyBuckets
	}
	histogramsMu.Lock()
	defer histogramsMu.Unlock()
	h, ok := histograms[name]
	if !ok {
		var err error
		h, err = otel.Meter(instrumentationName).Float64Histogram(name,
			metric.WithDescription(help), metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(boundaries...))
		if err != nil {
			return func(context.Context, float64, ...any) {}
		}
		histograms[name] = h
	}
	return func(ctx context.Context, value float64, attrs ...any) {
		h.Record(ctx, value, metric.WithAttributes(kv(attrs)...))
	}
}
