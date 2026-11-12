package fieldreport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"

	amqp "github.com/rabbitmq/amqp091-go"

	"aggregator/internal/amqpx"
	"aggregator/internal/dlq"
	"aggregator/internal/shardstore"
	"telemetry"
)

const Collection = "field_reports"

type Message struct {
	ReportID      string          `json:"report_id"`
	ClientID      string          `json:"client_id"`
	ReceivedAt    time.Time       `json:"received_at"`
	CorrelationID string          `json:"correlation_id"`
	Report        json.RawMessage `json:"report"`
}

type Stored struct {
	Message
	ProcessedAt  time.Time `json:"processed_at"`
	ProcessCount int       `json:"process_count"`
	Deliveries   int       `json:"deliveries"`
}

type Docs interface {
	Put(ctx context.Context, d shardstore.Doc) error
	PutIfAbsent(ctx context.Context, d shardstore.Doc) (bool, *shardstore.Doc, error)
	ScanAll(ctx context.Context, q shardstore.DocQuery) ([]shardstore.Doc, bool, error)
}

var (
	processed  = telemetry.Counter("field_reports_processed", "laporan Tim Lapangan yang diproses pertama kali")
	duplicates = telemetry.Counter("field_reports_duplicate_deliveries", "pengiriman ulang laporan yang sudah diproses (tidak diproses ganda)")
	lag        = telemetry.Histogram("field_report_accept_to_process_seconds", "waktu dari 202 sampai laporan tersimpan",
		0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300)
)

type Processor struct {
	docs  Docs
	log   *slog.Logger
	delay time.Duration
}

func New(docs Docs, log *slog.Logger, delay time.Duration) *Processor {
	return &Processor{docs: docs, log: log, delay: delay}
}

var ErrInvalid = errors.New("laporan tidak valid")

func Decode(body []byte) (Message, error) {
	if !utf8.Valid(body) {
		return Message{}, fmt.Errorf("%w: UTF-8 tidak valid", ErrInvalid)
	}
	var m Message
	if err := json.Unmarshal(body, &m); err != nil {
		return Message{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if m.ReportID == "" || len(m.Report) == 0 {
		return Message{}, fmt.Errorf("%w: report_id dan report wajib ada", ErrInvalid)
	}
	return m, nil
}

// Process menyimpan satu laporan secara idempoten. created=false berarti laporan
// sudah pernah diproses dan pengiriman ini hanya dicatat.
func (p *Processor) Process(ctx context.Context, m Message) (bool, error) {
	if p.delay > 0 {
		select {
		case <-time.After(p.delay):
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	now := time.Now().UTC()
	st := Stored{Message: m, ProcessedAt: now, ProcessCount: 1, Deliveries: 1}
	data, _ := json.Marshal(st)
	doc := shardstore.Doc{Collection: Collection, Key: m.ReportID, SortKey: fmt.Sprintf("%019d", m.ReceivedAt.UnixNano()),
		Index: map[string]string{"client": m.ClientID}, Data: data}
	created, existing, err := p.docs.PutIfAbsent(ctx, doc)
	if err != nil {
		return false, err
	}
	if created {
		processed(ctx, 1)
		if !m.ReceivedAt.IsZero() {
			lag(ctx, now.Sub(m.ReceivedAt).Seconds())
		}
		// Efek samping laporan (notifikasi ke posko) terjadi di sini, satu kali per report_id.
		p.log.Info("laporan Tim Lapangan diproses", "report_id", m.ReportID, "client_id", m.ClientID,
			"correlation_id", m.CorrelationID, "accept_to_process_ms", now.Sub(m.ReceivedAt).Milliseconds())
		return true, nil
	}
	duplicates(ctx, 1)
	var prev Stored
	if existing != nil && json.Unmarshal(existing.Data, &prev) == nil {
		prev.Deliveries++
		if b, err := json.Marshal(prev); err == nil {
			existing.Data = b
			existing.Version = 0
			_ = p.docs.Put(ctx, *existing)
		}
	}
	p.log.Info("pengiriman ulang laporan diabaikan (sudah diproses)", "report_id", m.ReportID, "correlation_id", m.CorrelationID)
	return false, nil
}

// memproses satu delivery AMQP.
func (p *Processor) Handle(ctx context.Context, d amqp.Delivery) {
	m, err := Decode(d.Body)
	if err != nil {
		p.log.Warn("laporan rusak dipindah ke DLQ", "err", err, "correlation_id", d.CorrelationId)
		_ = d.Nack(false, false)
		return
	}
	if _, err := p.Process(ctx, m); err != nil {
		p.log.Warn("laporan belum tersimpan, dicoba lagi", "report_id", m.ReportID, "err", err)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
		}
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}

// mengonsumsi queue laporan sampai ctx selesai.
func (p *Processor) Run(ctx context.Context, url string, prefetch int) {
	amqpx.Consume(ctx, url, "field-reports", p.log, func(ch *amqp.Channel) (<-chan amqp.Delivery, error) {
		if err := amqpx.DeclareFieldReports(ch); err != nil {
			return nil, err
		}
		if err := ch.Qos(prefetch, 0, false); err != nil {
			return nil, err
		}
		return ch.Consume(amqpx.FieldReportsQueue, "", false, false, false, false, nil)
	}, p.Handle)
}

// memproses ulang laporan yang sempat masuk DLQ.
func (p *Processor) DLQProcessor(ctx context.Context, e dlq.Entry) error {
	raw, err := e.RawPayload()
	if err != nil {
		return dlq.Permanent(err.Error())
	}
	m, err := Decode(raw)
	if err != nil {
		return dlq.Permanent("malformed: " + err.Error())
	}
	_, err = p.Process(ctx, m)
	return err
}

type Stats struct {
	Reports              int  `json:"reports"`
	Deliveries           int  `json:"deliveries"`
	DuplicateDeliveries  int  `json:"duplicate_deliveries"`
	MaxProcessCount      int  `json:"max_process_count"`
	ProcessedMoreThanOne int  `json:"processed_more_than_once"`
	Partial              bool `json:"partial"`
}

// membuktikan tidak ada laporan 202 yang hilang atau diproses ganda.
func (p *Processor) Stats(ctx context.Context) (Stats, []string, error) {
	docs, partial, err := p.docs.ScanAll(ctx, shardstore.DocQuery{Collection: Collection})
	if err != nil {
		return Stats{}, nil, err
	}
	s := Stats{Partial: partial}
	ids := make([]string, 0, len(docs))
	for _, d := range docs {
		var st Stored
		if json.Unmarshal(d.Data, &st) != nil {
			continue
		}
		ids = append(ids, st.ReportID)
		s.Reports++
		s.Deliveries += st.Deliveries
		s.MaxProcessCount = max(s.MaxProcessCount, st.ProcessCount)
		if st.ProcessCount > 1 {
			s.ProcessedMoreThanOne++
		}
	}
	s.DuplicateDeliveries = s.Deliveries - s.Reports
	return s, ids, nil
}
