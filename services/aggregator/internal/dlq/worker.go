package dlq

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"telemetry"
)

type Processor func(ctx context.Context, e Entry) error

var (
	retried     = telemetry.Counter("dlq_retry_attempts", "percobaan ulang otomatis atas pesan berstatus retrying")
	recoveredC  = telemetry.Counter("dlq_recovered", "pesan gagal yang berhasil dipulihkan")
	parkedTotal = telemetry.Counter("dlq_parked", "pesan yang dipindah ke status parked")
)

func (d *DLQ) RegisterMetrics(ctx context.Context) {
	var cached map[string]int
	var at time.Time
	telemetry.GaugeFunc("dlq_messages", "jumlah pesan di DLQ per status", func() []telemetry.Sample {
		if time.Since(at) > 15*time.Second {
			c, cancel := context.WithTimeout(ctx, 3*time.Second)
			if counts, err := d.Counts(c); err == nil {
				cached, at = counts, time.Now()
			}
			cancel()
		}
		out := make([]telemetry.Sample, 0, len(cached))
		for status, n := range cached {
			out = append(out, telemetry.Sample{Value: float64(n), Attrs: []any{"status", status}})
		}
		return out
	})
}

// menjalankan processor dan memperbarui entri sesuai hasilnya.
func (d *DLQ) attempt(ctx context.Context, e Entry, proc Processor) (ok bool, err error) {
	perr := proc(ctx, e)
	if perr == nil {
		recoveredC(ctx, 1, "source", e.Source, "kind", e.Kind)
		return true, d.Recovered(ctx, e.MessageID)
	}
	e.Attempts++
	if reason, permanent := IsPermanent(perr); permanent {
		e.Status, e.Reason, e.NextAttemptAt = StatusParked, reason, time.Time{}
		parkedTotal(ctx, 1, "source", e.Source)
	} else if e.Attempts >= d.MaxAttempts {
		e.Status, e.Reason, e.NextAttemptAt = StatusParked, "retries_exhausted: "+perr.Error(), time.Time{}
		parkedTotal(ctx, 1, "source", e.Source)
	} else {
		e.Status, e.Reason = StatusRetrying, "transient: "+perr.Error()
		e.NextAttemptAt = d.now().UTC().Add(d.backoff(e.Attempts))
	}
	doc, err := d.toDoc(e)
	if err != nil {
		return false, err
	}
	return false, d.docs.Put(ctx, doc)
}

// mencoba lagi semua pesan retrying yang sudah jatuh tempo.
func (d *DLQ) RetryDue(ctx context.Context, procs map[string]Processor) (int, error) {
	entries, err := d.all(ctx, map[string]string{"status": StatusRetrying})
	if err != nil {
		return 0, err
	}
	now := d.now().UTC()
	sortEntries(entries)
	recovered := 0
	for _, e := range entries {
		if e.NextAttemptAt.After(now) {
			continue
		}
		proc, ok := procs[e.Kind]
		if !ok {
			continue
		}
		retried(ctx, 1, "source", e.Source)
		ok, err := d.attempt(ctx, e, proc)
		if err != nil {
			return recovered, err
		}
		if ok {
			recovered++
		}
	}
	return recovered, nil
}

// RunRetry menjalankan RetryDue berkala. leader menentukan apakah instance ini yang
// bertugas. nil berarti selalu (aman karena pemrosesan idempoten).
func (d *DLQ) RunRetry(ctx context.Context, interval time.Duration, procs map[string]Processor, leader func() bool, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if leader != nil && !leader() {
				continue
			}
			rctx, cancel := context.WithTimeout(ctx, interval*4)
			if n, err := d.RetryDue(rctx, procs); err != nil {
				log.Warn("retry DLQ gagal", "err", err)
			} else if n > 0 {
				log.Info("pesan DLQ pulih lewat retry otomatis", "recovered", n)
			}
			cancel()
		}
	}
}

type ReprocessResult struct {
	Source       string   `json:"source"`
	Considered   int      `json:"considered"`
	Recovered    int      `json:"recovered"`
	StillParked  int      `json:"still_parked"`
	Blocked      int      `json:"blocked"`
	Skipped      int      `json:"skipped_unsupported"`
	RecoveredIDs []string `json:"recovered_ids"`
}

func sortEntries(entries []Entry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].SourceRefID != entries[j].SourceRefID {
			return entries[i].SourceRefID < entries[j].SourceRefID
		}
		if !entries[i].ReceivedAt.Equal(entries[j].ReceivedAt) {
			return entries[i].ReceivedAt.Before(entries[j].ReceivedAt)
		}
		return entries[i].MessageID < entries[j].MessageID
	})
}

func (d *DLQ) Reprocess(ctx context.Context, source string, procs map[string]Processor) (ReprocessResult, error) {
	eq := map[string]string{}
	if source != "" {
		eq["source"] = source
	}
	entries, err := d.all(ctx, eq)
	if err != nil {
		return ReprocessResult{}, err
	}
	sortEntries(entries)
	res := ReprocessResult{Source: source, RecoveredIDs: []string{}}
	blocked := map[string]bool{}
	for _, e := range entries {
		res.Considered++
		proc, ok := procs[e.Kind]
		if !ok {
			res.Skipped++
			res.StillParked++
			continue
		}
		key := e.Source + "|" + e.SourceRefID
		if e.SourceRefID != "" && blocked[key] {
			res.Blocked++
			res.StillParked++
			continue
		}
		ok, err := d.attempt(ctx, e, proc)
		if err != nil {
			return res, err
		}
		if ok {
			res.Recovered++
			res.RecoveredIDs = append(res.RecoveredIDs, e.MessageID)
			continue
		}
		res.StillParked++
		if e.SourceRefID != "" {
			blocked[key] = true
		}
	}
	return res, nil
}
