package dlq

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"aggregator/internal/audit"
	"aggregator/internal/shardstore"
)

const Collection = "unprocessed"

const (
	StatusRetrying = "retrying"
	StatusParked   = "parked"
)

// Jenis pesan menentukan cara memprosesnya ulang.
const (
	KindPVMBGReport = "pvmbg_report" // satu report PVMBG yang terbaca sebagai objek JSON
	KindPVMBGRaw    = "pvmbg_raw"    // potongan respons PVMBG yang tidak terbaca
	KindAMQP        = "amqp"         // pesan broker yang ditolak konsumer
)

type Entry struct {
	MessageID       string    `json:"message_id"`
	Source          string    `json:"source"`
	Kind            string    `json:"kind"`
	ReceivedAt      time.Time `json:"received_at"`
	Reason          string    `json:"reason"`
	Retryable       bool      `json:"retryable"`
	Attempts        int       `json:"attempts"`
	Status          string    `json:"status"`
	SourceRefID     string    `json:"source_ref_id,omitempty"`
	Payload         string    `json:"payload"`
	PayloadEncoding string    `json:"payload_encoding"`
	PayloadBytes    int       `json:"payload_bytes"`
	NextAttemptAt   time.Time `json:"next_attempt_at,omitempty"`
	CorrelationID   string    `json:"correlation_id,omitempty"`
	Origin          string    `json:"origin,omitempty"`
}

func (e Entry) RawPayload() ([]byte, error) {
	if e.PayloadEncoding == "base64" {
		return base64.StdEncoding.DecodeString(e.Payload)
	}
	return []byte(e.Payload), nil
}

func (e *Entry) SetPayload(raw []byte) {
	e.PayloadBytes = len(raw)
	if utf8.Valid(raw) {
		e.Payload, e.PayloadEncoding = string(raw), "utf8"
		return
	}
	e.Payload, e.PayloadEncoding = base64.StdEncoding.EncodeToString(raw), "base64"
}

func MessageID(source string, raw []byte) string {
	h := sha256.New()
	h.Write([]byte(source))
	h.Write([]byte{0})
	h.Write(raw)
	return "msg-" + hex.EncodeToString(h.Sum(nil))[:16]
}

type PermanentError struct{ Reason string }

func (e *PermanentError) Error() string { return e.Reason }

func Permanent(reason string) error { return &PermanentError{Reason: reason} }

func IsPermanent(err error) (string, bool) {
	var pe *PermanentError
	if errors.As(err, &pe) {
		return pe.Reason, true
	}
	return "", false
}

// bagian DocStore yang dipakai DLQ.
type Docs interface {
	Put(ctx context.Context, d shardstore.Doc) error
	Get(ctx context.Context, collection, key string) (*shardstore.Doc, error)
	Delete(ctx context.Context, collection, key string) error
	Scan(ctx context.Context, q shardstore.DocQuery) (shardstore.DocPage, error)
	ScanAll(ctx context.Context, q shardstore.DocQuery) ([]shardstore.Doc, bool, error)
}

type DLQ struct {
	docs        Docs
	now         func() time.Time
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

func New(docs Docs) *DLQ {
	return &DLQ{docs: docs, now: time.Now, MaxAttempts: 8, BaseBackoff: time.Second, MaxBackoff: 5 * time.Minute}
}

func sortKey(t time.Time) string { return fmt.Sprintf("%019d", t.UTC().UnixNano()) }

func (d *DLQ) toDoc(e Entry) (shardstore.Doc, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return shardstore.Doc{}, err
	}
	return shardstore.Doc{
		Collection: Collection,
		Key:        e.MessageID,
		SortKey:    sortKey(e.ReceivedAt),
		Index:      map[string]string{"source": e.Source, "status": e.Status, "ref": e.SourceRefID},
		Data:       data,
	}, nil
}

func fromDoc(doc shardstore.Doc) (Entry, error) {
	var e Entry
	err := json.Unmarshal(doc.Data, &e)
	return e, err
}

func (d *DLQ) backoff(attempts int) time.Duration {
	b := d.BaseBackoff
	for i := 1; i < attempts && b < d.MaxBackoff; i++ {
		b *= 2
	}
	return min(b, d.MaxBackoff)
}

// Record mencatat pesan gagal. Apabila pesan yang sama sudah tercatat, jumlah percobaan
// dan waktu terima pertama dipertahankan.
func (d *DLQ) Record(ctx context.Context, e Entry) error {
	if e.MessageID == "" {
		raw, _ := e.RawPayload()
		e.MessageID = MessageID(e.Source, raw)
	}
	now := d.now().UTC()
	if existing, err := d.docs.Get(ctx, Collection, e.MessageID); err != nil {
		return err
	} else if existing != nil {
		prev, err := fromDoc(*existing)
		if err == nil {
			e.ReceivedAt = prev.ReceivedAt
			e.Attempts = max(prev.Attempts, e.Attempts)
			if prev.Status == StatusParked && e.Status == StatusRetrying {
				// Pesan yang sudah di-park tidak kembali ke retry karena diterima ulang.
				e.Status, e.Retryable, e.Reason = prev.Status, prev.Retryable, prev.Reason
			}
		}
	}
	if e.ReceivedAt.IsZero() {
		e.ReceivedAt = now
	}
	if e.Attempts < 1 {
		e.Attempts = 1
	}
	e.Retryable = e.Status == StatusRetrying
	if e.Status == StatusRetrying && e.NextAttemptAt.IsZero() {
		e.NextAttemptAt = now.Add(d.backoff(e.Attempts))
	}
	doc, err := d.toDoc(e)
	if err != nil {
		return err
	}
	return d.docs.Put(ctx, doc)
}

func (d *DLQ) Get(ctx context.Context, id string) (*Entry, error) {
	doc, err := d.docs.Get(ctx, Collection, id)
	if err != nil || doc == nil {
		return nil, err
	}
	e, err := fromDoc(*doc)
	return &e, err
}

// menghapus pesan yang sudah berhasil diproses.
func (d *DLQ) Recovered(ctx context.Context, id string) error {
	return d.docs.Delete(ctx, Collection, id)
}

// HasParked memberi tahu apakah source_ref_id ini masih punya pesan parked, sehingga
// pesan berikutnya dengan kunci yang sama harus menunggu agar urutannya terjaga.
func (d *DLQ) HasParked(ctx context.Context, source, ref string) (bool, error) {
	if ref == "" {
		return false, nil
	}
	page, err := d.docs.Scan(ctx, shardstore.DocQuery{Collection: Collection, Limit: 1,
		Eq: map[string]string{"source": source, "ref": ref, "status": StatusParked}, WithoutData: true})
	if err != nil {
		return false, err
	}
	return len(page.Docs) > 0, nil
}

// mengembalikan entri lengkap (termasuk payload) untuk pemeriksaan Internal Ops.
func (d *DLQ) List(ctx context.Context, source, status string, limit int, cursor string) ([]Entry, string, bool, error) {
	afterS, afterK, err := shardstore.DecodeDocCursor(cursor)
	if err != nil {
		return nil, "", false, err
	}
	eq := map[string]string{}
	if source != "" {
		eq["source"] = source
	}
	if status != "" {
		eq["status"] = status
	}
	page, err := d.docs.Scan(ctx, shardstore.DocQuery{Collection: Collection, Eq: eq, Limit: limit, AfterSort: afterS, AfterKey: afterK})
	if err != nil {
		return nil, "", false, err
	}
	out := make([]Entry, 0, len(page.Docs))
	for _, doc := range page.Docs {
		if e, err := fromDoc(doc); err == nil {
			out = append(out, e)
		}
	}
	return out, page.NextCursor, page.Partial, nil
}

func (d *DLQ) ListUnprocessed(ctx context.Context, source string, limit int, cursor string) ([]audit.Message, string, error) {
	entries, next, _, err := d.List(ctx, source, "", limit, cursor)
	if err != nil {
		return nil, "", err
	}
	out := make([]audit.Message, 0, len(entries))
	for _, e := range entries {
		out = append(out, audit.Message{MessageID: e.MessageID, Source: e.Source, ReceivedAt: e.ReceivedAt,
			Reason: e.Reason, Retryable: e.Retryable, Attempts: e.Attempts, Status: e.Status})
	}
	return out, next, nil
}

func (d *DLQ) all(ctx context.Context, eq map[string]string) ([]Entry, error) {
	docs, _, err := d.docs.ScanAll(ctx, shardstore.DocQuery{Collection: Collection, Eq: eq})
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0, len(docs))
	for _, doc := range docs {
		if e, err := fromDoc(doc); err == nil {
			out = append(out, e)
		}
	}
	return out, nil
}

// mengembalikan jumlah pesan per status (untuk metrik).
func (d *DLQ) Counts(ctx context.Context) (map[string]int, error) {
	entries, err := d.all(ctx, nil)
	if err != nil {
		return nil, err
	}
	out := map[string]int{StatusRetrying: 0, StatusParked: 0}
	for _, e := range entries {
		out[e.Status]++
	}
	return out, nil
}
