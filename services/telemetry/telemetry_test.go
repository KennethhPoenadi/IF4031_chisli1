package telemetry

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newProvider(t *testing.T, service string) (*Provider, *tracetest.InMemoryExporter) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	p, err := build(settings{service: service, ratio: 1}, exp, true, quiet())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p, exp
}

func attr(span tracetest.SpanStub, key string) string {
	for _, a := range span.Attributes {
		if string(a.Key) == key {
			return a.Value.String()
		}
	}
	return ""
}

func TestOneTraceFollowsARequestAcrossTwoServices(t *testing.T) {
	aggP, aggExp := newProvider(t, "aggregator")
	apiP, apiExp := newProvider(t, "client-facing-api")

	aggregator := httptest.NewServer(aggP.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(CorrelationHeader, "corr-123")
		w.WriteHeader(http.StatusOK)
	})))
	defer aggregator.Close()

	client := &http.Client{Transport: apiP.Transport(nil)}
	api := httptest.NewServer(apiP.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, aggregator.URL+"/hazard-events", nil)
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		resp.Body.Close()
		w.Header().Set(CorrelationHeader, "corr-123")
	})))
	defer api.Close()

	resp, err := http.Get(api.URL + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	apiSpans, aggSpans := apiExp.GetSpans(), aggExp.GetSpans()
	if len(aggSpans) != 1 {
		t.Fatalf("aggregator span = %d, want 1", len(aggSpans))
	}
	var serverSpan, clientSpan tracetest.SpanStub
	for _, s := range apiSpans {
		switch s.SpanKind {
		case trace.SpanKindServer:
			serverSpan = s
		case trace.SpanKindClient:
			clientSpan = s
		}
	}
	if serverSpan.Name == "" || clientSpan.Name == "" {
		t.Fatalf("client-facing harus punya span server dan client: %+v", apiSpans)
	}
	traceID := serverSpan.SpanContext.TraceID()
	if clientSpan.SpanContext.TraceID() != traceID || aggSpans[0].SpanContext.TraceID() != traceID {
		t.Fatal("seluruh span harus berada pada satu trace yang sama lintas service")
	}
	if clientSpan.Parent.SpanID() != serverSpan.SpanContext.SpanID() {
		t.Fatal("span panggilan keluar harus anak dari span permintaan masuk")
	}
	if aggSpans[0].Parent.SpanID() != clientSpan.SpanContext.SpanID() {
		t.Fatal("span aggregator harus anak dari span panggilan client-facing")
	}
	if got := attr(serverSpan, "correlation.id"); got != "corr-123" {
		t.Fatalf("correlation.id pada span = %q, want corr-123", got)
	}
	if aggSpans[0].Resource.Attributes() == nil {
		t.Fatal("resource tidak terisi")
	}
}

func TestHealthChecksAreNotTracedOrMeasured(t *testing.T) {
	p, exp := newProvider(t, "svc")
	srv := httptest.NewServer(p.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {})))
	defer srv.Close()
	for _, path := range []string{"/health", "/healthz"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if n := len(exp.GetSpans()); n != 0 {
		t.Fatalf("health check menghasilkan %d span, seharusnya 0", n)
	}
	resp, _ := http.Get(srv.URL + "/data")
	resp.Body.Close()
	if n := len(exp.GetSpans()); n != 1 {
		t.Fatalf("permintaan biasa menghasilkan %d span, want 1", n)
	}
}

func TestMetricsExposeRateErrorsAndDuration(t *testing.T) {
	p, _ := newProvider(t, "svc")
	srv := httptest.NewServer(p.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/boom" {
			http.Error(w, "x", http.StatusInternalServerError)
		}
	})))
	defer srv.Close()
	for _, path := range []string{"/ok", "/ok", "/boom"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}

	rec := httptest.NewRecorder()
	p.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"http_server_request_duration_seconds_bucket",
		"http_server_request_duration_seconds_count",
		`http_response_status_code="500"`,
		"go_goroutines",
		"process_resident_memory_bytes",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics tidak memuat %q", want)
		}
	}
}

func TestSamplingRatioZeroStillPropagatesButExportsNothing(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	p, err := build(settings{service: "svc", ratio: 0}, exp, true, quiet())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	srv := httptest.NewServer(p.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})))
	defer srv.Close()
	resp, _ := http.Get(srv.URL + "/x")
	resp.Body.Close()
	if n := len(exp.GetSpans()); n != 0 {
		t.Fatalf("rasio 0 mengekspor %d span", n)
	}
}

func TestAMQPHeadersCarryTheTraceContext(t *testing.T) {
	p, _ := newProvider(t, "publisher")
	ctx, span := p.Tracer("test").Start(context.Background(), "publish")
	defer span.End()

	headers := map[string]any{}
	Inject(ctx, headers)
	if headers["traceparent"] == nil {
		t.Fatalf("header traceparent tidak terisi: %v", headers)
	}

	got := trace.SpanContextFromContext(Extract(context.Background(), headers))
	if !got.IsValid() || got.TraceID() != span.SpanContext().TraceID() {
		t.Fatalf("trace id setelah ekstraksi = %v, want %v", got.TraceID(), span.SpanContext().TraceID())
	}
	if !got.IsRemote() {
		t.Fatal("konteks hasil ekstraksi harus ditandai remote")
	}
}

func TestExtractIgnoresMissingOrNonStringHeaders(t *testing.T) {
	_, _ = newProvider(t, "consumer")
	ctx := Extract(context.Background(), map[string]any{"traceparent": 42, "other": nil})
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("header yang bukan string tidak boleh menghasilkan konteks trace")
	}
	if len(MapCarrier(map[string]any{"a": "1", "b": 2}).Keys()) != 2 {
		t.Fatal("Keys harus mengembalikan seluruh kunci")
	}
}

func TestLogAttrsOnlyWhenASpanIsActive(t *testing.T) {
	p, _ := newProvider(t, "svc")
	if got := LogAttrs(context.Background()); got != nil {
		t.Fatalf("tanpa span: %v, want nil", got)
	}
	ctx, span := p.Tracer("t").Start(context.Background(), "op")
	defer span.End()
	got := LogAttrs(ctx)
	if len(got) != 4 || got[0] != "trace_id" || got[2] != "span_id" || got[1] != span.SpanContext().TraceID().String() {
		t.Fatalf("LogAttrs = %v", got)
	}
}

func TestInitWithoutCollectorWorksAndDoesNotExport(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("TELEMETRY_METRICS_ADDR", "off")
	p, err := Init(context.Background(), "svc", quiet())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	if p.Exporting {
		t.Fatal("tanpa endpoint, trace tidak boleh diekspor")
	}
	srv := httptest.NewServer(p.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})))
	defer srv.Close()
	if resp, err := http.Get(srv.URL + "/x"); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("service harus tetap berjalan tanpa kolektor: %v", err)
	}
}

func TestSettingsFromEnv(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")
	t.Setenv("TELEMETRY_METRICS_ADDR", ":9999")
	t.Setenv("POD_NAME", "aggregator-abc")
	t.Setenv("NODE_NAME", "node-b")
	s := settingsFromEnv("aggregator")
	if s.ratio != 0.25 || s.metrics != ":9999" || len(s.resources) != 2 {
		t.Fatalf("settings = %+v", s)
	}
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "banyak")
	if settingsFromEnv("x").ratio != 1 {
		t.Fatal("rasio tidak valid harus kembali ke 1")
	}
}

var _ sdktrace.SpanExporter = (*tracetest.InMemoryExporter)(nil)

func TestStartSpanRecordsErrorsAndNestsUnderTheCaller(t *testing.T) {
	_, exp := newProvider(t, "svc")
	ctx, endParent := StartSpan(context.Background(), "ingest", "hazard_id", "hz-1", "count", 3)
	_, endChild := StartSpan(ctx, "store.save", "unit", "store-0", "ignored")
	endChild(context.DeadlineExceeded)
	endParent(nil)

	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("span = %d, want 2", len(spans))
	}
	child, parent := spans[0], spans[1]
	if child.Name != "store.save" || child.Parent.SpanID() != parent.SpanContext.SpanID() {
		t.Fatalf("span anak tidak bersarang di bawah induk: %+v", child)
	}
	if child.Status.Code.String() != "Error" || len(child.Events) == 0 {
		t.Fatalf("kegagalan harus tercatat pada span: status=%v events=%d", child.Status, len(child.Events))
	}
	if parent.Status.Code.String() == "Error" {
		t.Fatal("span induk tidak boleh ikut gagal")
	}
	if attr(parent, "hazard_id") != "hz-1" || attr(parent, "count") != "3" || attr(child, "unit") != "store-0" {
		t.Fatal("atribut span tidak terisi")
	}
}

func TestProducerAndConsumerSpansShareOneTrace(t *testing.T) {
	_, exp := newProvider(t, "svc")
	headers := map[string]any{}
	pctx, endProducer := StartProducerSpan(context.Background(), "publish hazard_events", headers, "messaging.system", "rabbitmq")
	endProducer(nil)

	_, endConsumer := StartConsumerSpan(context.Background(), "consume notifier-queue", headers)
	endConsumer(nil)

	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("span = %d, want 2", len(spans))
	}
	producer, consumer := spans[0], spans[1]
	if producer.SpanKind != trace.SpanKindProducer || consumer.SpanKind != trace.SpanKindConsumer {
		t.Fatal("jenis span producer dan consumer salah")
	}
	if consumer.SpanContext.TraceID() != producer.SpanContext.TraceID() || consumer.Parent.SpanID() != producer.SpanContext.SpanID() {
		t.Fatal("pemrosesan pesan harus menjadi lanjutan trace pengirim")
	}
	_ = pctx
}

func TestCounterAndGaugeAreExposedToPrometheus(t *testing.T) {
	p, _ := newProvider(t, "svc")
	add := Counter("hazard_events_ingested", "event yang berhasil disimpan")
	add(context.Background(), 2, "source", "BMKG")
	add(context.Background(), 1, "source", "BMKG")
	GaugeFunc("store_units_unavailable", "unit yang tidak terjangkau", func() []Sample {
		return []Sample{{Value: 1, Attrs: []any{"unit", "store-1"}}}
	})

	rec := httptest.NewRecorder()
	p.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{`hazard_events_ingested_total{`, `source="BMKG"`, `store_units_unavailable{`, `unit="store-1"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics tidak memuat %q\n%s", want, body)
		}
	}
	if !strings.Contains(body, "} 3") {
		t.Fatal("counter harus menjumlahkan 2 + 1 = 3")
	}
}

func TestHelpersNeverPanicOnBadInput(t *testing.T) {
	_, _ = newProvider(t, "svc")
	_, end := StartSpan(context.Background(), "op", 7, "nilai", "kunci-tanpa-nilai")
	end(nil)
	if got := kv([]any{"a", struct{}{}, 5, "x", "b", true}); len(got) != 1 {
		t.Fatalf("hanya pasangan valid yang boleh dipakai, dapat %v", got)
	}
}

func TestHistogramExposesBucketsForPercentiles(t *testing.T) {
	p, _ := newProvider(t, "svc")
	observe := Histogram("notification_latency", "latensi dari event masuk sampai notifikasi")
	for _, v := range []float64{0.05, 0.4, 4.2, 6.5} {
		observe(context.Background(), v, "outcome", "alert")
	}
	rec := httptest.NewRecorder()
	p.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{"notification_latency_seconds_bucket", `le="5"`, "notification_latency_seconds_count", `outcome="alert"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics tidak memuat %q\n%s", want, body)
		}
	}
	if !strings.Contains(body, "notification_latency_seconds_count{") || !strings.Contains(body, "} 4") {
		t.Fatal("jumlah pengamatan harus 4")
	}
}

func TestInitExportsTracesToTheOTLPCollector(t *testing.T) {
	type hit struct {
		path, contentType string
		bytes             int
	}
	hits := make(chan hit, 8)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		hits <- hit{r.URL.Path, r.Header.Get("Content-Type"), len(body)}
		w.WriteHeader(http.StatusOK)
	}))
	defer collector.Close()

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	t.Setenv("TELEMETRY_METRICS_ADDR", "off")
	p, err := Init(context.Background(), "aggregator", quiet())
	if err != nil {
		t.Fatal(err)
	}
	if !p.Exporting {
		t.Fatal("dengan endpoint, trace harus diekspor")
	}

	srv := httptest.NewServer(p.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/hazard-events")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case h := <-hits:
		if h.path != "/v1/traces" {
			t.Fatalf("path OTLP = %q, want /v1/traces", h.path)
		}
		if h.contentType != "application/x-protobuf" || h.bytes == 0 {
			t.Fatalf("muatan OTLP tidak valid: %+v", h)
		}
	default:
		t.Fatal("kolektor tidak menerima trace apa pun")
	}
}

func TestExportFailureDoesNotBreakTheService(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("TELEMETRY_METRICS_ADDR", "off")
	p, err := Init(context.Background(), "svc", quiet())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(p.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })))
	defer srv.Close()
	for i := 0; i < 20; i++ {
		resp, err := http.Get(srv.URL + "/x")
		if err != nil || resp.StatusCode != http.StatusTeapot {
			t.Fatalf("permintaan %d gagal walau kolektor mati: %v", i, err)
		}
		resp.Body.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = p.Shutdown(ctx)
}
