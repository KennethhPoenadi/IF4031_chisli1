// Package telemetry adalah instrumentasi bersama seluruh service: trace terdistribusi (OpenTelemetry, OTLP),
// metrik (OpenTelemetry yang diekspos sebagai Prometheus), dan atribut untuk menautkan log ke trace.
//
// Prinsipnya: service tetap berjalan normal bila kolektor tidak ada. Tanpa OTEL_EXPORTER_OTLP_ENDPOINT, trace
// tidak diekspor tetapi konteksnya tetap dipropagasi ke service berikutnya.
//
// Konfigurasi lewat variabel lingkungan standar OpenTelemetry:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT  alamat kolektor OTLP/HTTP, mis. http://alloy.observability.svc:4318 (kosong = tidak ekspor)
//	OTEL_TRACES_SAMPLER_ARG      rasio sampling 0..1 (bawaan 1). Turunkan saat load test besar
//	TELEMETRY_METRICS_ADDR       alamat /metrics Prometheus (bawaan :9102, "off" untuk mematikan)
//	POD_NAME, POD_NAMESPACE, NODE_NAME   atribut resource, diisi chart dari downward API
package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// DefaultMetricsAddr adalah alamat bawaan endpoint /metrics, terpisah dari port aplikasi agar tidak terkena
// autentikasi dan NetworkPolicy milik API.
const DefaultMetricsAddr = ":9102"

// CorrelationHeader adalah header correlation ID yang sudah dipakai sejak M1.
const CorrelationHeader = "X-Correlation-ID"

// Provider memegang trace, metrik, dan propagator sebuah service.
type Provider struct {
	service string
	tp      *sdktrace.TracerProvider
	mp      *sdkmetric.MeterProvider
	reg     *prometheus.Registry
	prop    propagation.TextMapPropagator
	srv     *http.Server
	log     *slog.Logger
	// Exporting true bila trace dikirim ke kolektor.
	Exporting bool
}

type settings struct {
	service   string
	endpoint  string
	ratio     float64
	metrics   string
	resources []attribute.KeyValue
}

func settingsFromEnv(service string) settings {
	s := settings{
		service:  service,
		endpoint: strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
		ratio:    1,
		metrics:  DefaultMetricsAddr,
	}
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			s.ratio = f
		}
	}
	if v, ok := os.LookupEnv("TELEMETRY_METRICS_ADDR"); ok {
		s.metrics = v
	}
	for env, key := range map[string]string{
		"POD_NAME": "k8s.pod.name", "POD_NAMESPACE": "k8s.namespace.name", "NODE_NAME": "k8s.node.name",
	} {
		if v := os.Getenv(env); v != "" {
			s.resources = append(s.resources, attribute.String(key, v))
		}
	}
	return s
}

// Init menyiapkan telemetry sebuah service dari variabel lingkungan dan memasang endpoint /metrics.
func Init(ctx context.Context, service string, log *slog.Logger) (*Provider, error) {
	s := settingsFromEnv(service)
	var exp sdktrace.SpanExporter
	if s.endpoint != "" {
		e, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(strings.TrimRight(s.endpoint, "/")+"/v1/traces"))
		if err != nil {
			return nil, err
		}
		exp = e
	}
	p, err := build(s, exp, false, log)
	if err != nil {
		return nil, err
	}
	if s.metrics != "" && s.metrics != "off" {
		p.serveMetrics(s.metrics)
	}
	return p, nil
}

// build merakit Provider. syncExport dipakai tes agar span langsung terekspor tanpa menunggu batch.
func build(s settings, exp sdktrace.SpanExporter, syncExport bool, log *slog.Logger) (*Provider, error) {
	if log == nil {
		log = slog.Default()
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(append([]attribute.KeyValue{
		attribute.String("service.name", s.service),
	}, s.resources...)...))
	if err != nil {
		return nil, err
	}

	tpOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(s.ratio))),
	}
	if exp != nil {
		if syncExport {
			tpOpts = append(tpOpts, sdktrace.WithSyncer(exp))
		} else {
			tpOpts = append(tpOpts, sdktrace.WithBatcher(exp,
				sdktrace.WithBatchTimeout(2*time.Second), sdktrace.WithExportTimeout(5*time.Second)))
		}
	}
	tp := sdktrace.NewTracerProvider(tpOpts...)

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reader, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithResource(res))

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(prop)

	return &Provider{service: s.service, tp: tp, mp: mp, reg: reg, prop: prop, log: log, Exporting: exp != nil}, nil
}

// MetricsHandler mengembalikan handler /metrics format Prometheus.
func (p *Provider) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(p.reg, promhttp.HandlerOpts{})
}

func (p *Provider) serveMetrics(addr string) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", p.MetricsHandler())
	p.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := p.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.log.Error("metrics endpoint stopped", "addr", addr, "err", err)
		}
	}()
}

// Shutdown mengosongkan span yang tersisa dan menutup endpoint /metrics.
func (p *Provider) Shutdown(ctx context.Context) error {
	var errs []error
	if p.srv != nil {
		errs = append(errs, p.srv.Shutdown(ctx))
	}
	errs = append(errs, p.tp.Shutdown(ctx), p.mp.Shutdown(ctx))
	return errors.Join(errs...)
}

// Meter mengembalikan meter untuk metrik bisnis buatan service sendiri.
func (p *Provider) Meter(name string) metric.Meter { return p.mp.Meter(name) }

// Tracer mengembalikan tracer untuk span buatan service sendiri (mis. pemrosesan pesan).
func (p *Provider) Tracer(name string) trace.Tracer { return p.tp.Tracer(name) }

// skipPath menandai path yang tidak perlu ditelusuri maupun diukur (health check berkala).
func skipPath(r *http.Request) bool {
	return r.URL.Path == "/health" || r.URL.Path == "/healthz" || r.URL.Path == "/metrics"
}

// Middleware membungkus handler server: membuat span per permintaan (melanjutkan traceparent dari pemanggil),
// mengukur latensi, error rate, dan throughput, serta menempelkan correlation ID ke span agar trace dapat
// dicari dari log (dan sebaliknya).
func (p *Provider) Middleware(next http.Handler) http.Handler {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		// Header ini baru diisi middleware correlation ID milik service, jadi dibaca setelah handler selesai.
		if id := w.Header().Get(CorrelationHeader); id != "" {
			trace.SpanFromContext(r.Context()).SetAttributes(attribute.String("correlation.id", id))
		}
	})
	return otelhttp.NewHandler(inner, p.service,
		otelhttp.WithTracerProvider(p.tp),
		otelhttp.WithMeterProvider(p.mp),
		otelhttp.WithPropagators(p.prop),
		otelhttp.WithFilter(func(r *http.Request) bool { return !skipPath(r) }),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method + " " + r.URL.Path }),
	)
}

// Transport membungkus RoundTripper klien keluar: setiap panggilan ke service lain menjadi span anak dan
// membawa traceparent, sehingga satu permintaan dapat diikuti lintas service.
func (p *Provider) Transport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return otelhttp.NewTransport(base,
		otelhttp.WithTracerProvider(p.tp),
		otelhttp.WithMeterProvider(p.mp),
		otelhttp.WithPropagators(p.prop),
	)
}

// LogAttrs mengembalikan trace_id dan span_id untuk ditambahkan ke log, atau nil bila tidak ada span aktif.
func LogAttrs(ctx context.Context) []any {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	return []any{"trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String()}
}

// MapCarrier menyesuaikan header AMQP (map[string]any) ke antarmuka propagasi OpenTelemetry.
type MapCarrier map[string]any

func (c MapCarrier) Get(key string) string {
	s, _ := c[key].(string)
	return s
}

func (c MapCarrier) Set(key, value string) { c[key] = value }

func (c MapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// Inject menaruh konteks trace pada header pesan sebelum dipublikasikan.
func Inject(ctx context.Context, headers map[string]any) {
	otel.GetTextMapPropagator().Inject(ctx, MapCarrier(headers))
}

// Extract membaca konteks trace dari header pesan yang diterima.
func Extract(ctx context.Context, headers map[string]any) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, MapCarrier(headers))
}
