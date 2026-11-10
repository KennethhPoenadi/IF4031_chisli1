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

// kv mengubah pasangan kunci, nilai (string, int, bool, float) menjadi atribut OpenTelemetry. Pasangan yang
// tidak lengkap atau bertipe lain diabaikan, agar salah ketik pada pemanggil tidak pernah menjatuhkan service.
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

// StartSpan memulai span anak dari ctx (bila ada) dan mengembalikan fungsi penutupnya. Pemanggil memanggil
// end(err) sekali: err yang tidak nil ditandai sebagai kegagalan pada span. Tanpa Init, span tidak berefek.
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

// StartConsumerSpan memulai span untuk pemrosesan sebuah pesan, dan menyambungkannya ke trace pengirim lewat
// header pesan (Inject di sisi pengirim).
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

// StartProducerSpan memulai span untuk pengiriman pesan dan menaruh konteks trace-nya pada headers.
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

// Counter mengembalikan fungsi penambah untuk metrik counter bisnis (mis. event yang diproses). Nama memakai
// huruf kecil dan garis bawah, dan akan diekspos Prometheus dengan akhiran _total.
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

// Sample adalah satu nilai gauge beserta label-nya.
type Sample struct {
	Value float64
	Attrs []any
}

// GaugeFunc mendaftarkan gauge yang nilainya dibaca saat Prometheus melakukan scrape (mis. jumlah unit
// penyimpanan yang tidak terjangkau). fn dipanggil pada setiap scrape dan harus cepat.
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

// LatencyBuckets adalah batas bucket (detik) untuk latensi alur peringatan, rapat di sekitar target 5 detik.
var LatencyBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 7.5, 10, 30, 60}

// Histogram mengembalikan fungsi pencatat untuk metrik histogram (mis. latensi dalam detik). boundaries kosong
// berarti LatencyBuckets. Dari histogram ini Prometheus menghitung p50, p95, dan p99.
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
