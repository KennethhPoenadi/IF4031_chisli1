package poller

import (
	"context"
	"errors"
	"time"

	"aggregator/internal/dlq"
	"aggregator/internal/hazard"
	"aggregator/internal/mapper"
	"aggregator/internal/pvmbgclient"
)

type DeadLetters interface {
	Record(ctx context.Context, e dlq.Entry) error
	HasParked(ctx context.Context, source, ref string) (bool, error)
}

type BatchVolcanicSource interface {
	VolcanicBatch(ctx context.Context, since string, correlationID string) (pvmbgclient.Batch, error)
}

type tolerantIngest struct {
	dead  DeadLetters
	rules mapper.Rules
}

func (p *Poller) WithDeadLetters(d DeadLetters, rules mapper.Rules) *Poller {
	p.tolerant = &tolerantIngest{dead: d, rules: rules}
	return p
}

const sourcePVMBG = string(hazard.SourcePVMBG)

func (p *Poller) pollPVMBGTolerant(ctx context.Context, src BatchVolcanicSource) {
	correlationID := newCorrelationID()
	now := time.Now().UTC().Format(cursorFormat)

	batch, err := src.VolcanicBatch(ctx, p.pvmbgSince, correlationID)
	if err != nil {
		p.markPVMBGUnavailable()
		if errors.Is(err, pvmbgclient.ErrOutage) {
			p.log.Warn("pvmbg is in outage, skipping this cycle, seismic data unaffected", "correlation_id", correlationID)
		} else {
			p.log.Warn("pvmbg poll failed", "err", err, "correlation_id", correlationID)
		}
		return
	}
	p.markPVMBGAvailable()
	t := p.tolerant

	for _, br := range batch.Broken {
		e := dlq.Entry{Source: sourcePVMBG, Kind: dlq.KindPVMBGRaw, Status: dlq.StatusParked, Reason: br.Reason,
			CorrelationID: correlationID, Origin: "GET /volcanic-reports"}
		e.SetPayload(br.Raw)
		if err := t.dead.Record(ctx, e); err != nil {
			p.log.Error("pesan rusak tidak dapat dicatat ke DLQ, cursor tidak maju", "err", err, "correlation_id", correlationID)
			return
		}
		p.log.Warn("pesan PVMBG rusak disimpan di DLQ", "reason", br.Reason, "bytes", len(br.Raw), "correlation_id", correlationID)
	}

	var lastReported time.Time
	for _, item := range batch.Items {
		if ts, ok := item.Report["reported_at"].(string); ok {
			if rt, err := time.Parse(time.RFC3339, ts); err == nil && rt.After(lastReported) {
				lastReported = rt
			}
		}
		ref, _ := item.Report["report_id"].(string)
		entry := func(status, reason string) dlq.Entry {
			e := dlq.Entry{Source: sourcePVMBG, Kind: dlq.KindPVMBGReport, Status: status, Reason: reason,
				SourceRefID: ref, CorrelationID: correlationID, Origin: "GET /volcanic-reports"}
			e.SetPayload(item.Raw)
			return e
		}

		blocked, err := t.dead.HasParked(ctx, sourcePVMBG, ref)
		if err != nil {
			p.log.Error("DLQ tidak terbaca, cursor tidak maju", "err", err, "correlation_id", correlationID)
			return
		}
		if blocked {
			if err := t.dead.Record(ctx, entry(dlq.StatusParked, "blocked_by_earlier: source_ref_id "+ref+" masih punya pesan parked")); err != nil {
				return
			}
			continue
		}

		event, mapErr := mapper.MapVolcanicReport(t.rules.Apply(item.Report))
		if mapErr != nil {
			reason := mapper.SchemaReason(mapErr)
			if err := t.dead.Record(ctx, entry(dlq.StatusParked, reason)); err != nil {
				p.log.Error("report gagal tidak dapat dicatat ke DLQ, cursor tidak maju", "err", err, "correlation_id", correlationID)
				return
			}
			p.log.Warn("report PVMBG di-park", "source_ref_id", ref, "reason", reason, "correlation_id", correlationID)
			continue
		}
		if err := p.persist(ctx, event, correlationID); err != nil {
			if err := t.dead.Record(ctx, entry(dlq.StatusRetrying, "transient: "+err.Error())); err != nil {
				p.log.Error("report gagal disimpan dan DLQ tidak terjangkau, cursor tidak maju", "err", err, "correlation_id", correlationID)
				return
			}
		}
	}

	if batch.Truncated {
		if !lastReported.IsZero() {
			p.pvmbgSince = lastReported.UTC().Format(time.RFC3339)
		}
		return
	}
	p.pvmbgSince = now
}

func (p *Poller) ProcessPVMBGEntry(ctx context.Context, e dlq.Entry) error {
	raw, err := e.RawPayload()
	if err != nil {
		return dlq.Permanent("malformed: payload tidak dapat dibaca: " + err.Error())
	}
	b := pvmbgclient.DecodeBatch(append(append([]byte{'['}, raw...), ']'), false)
	if len(b.Items) != 1 || len(b.Broken) > 0 {
		reason := e.Reason
		if len(b.Broken) > 0 {
			reason = b.Broken[0].Reason
		}
		return dlq.Permanent(reason)
	}
	var rules mapper.Rules
	if p.tolerant != nil {
		rules = p.tolerant.rules
	}
	event, err := mapper.MapVolcanicReport(rules.Apply(b.Items[0].Report))
	if err != nil {
		return dlq.Permanent(mapper.SchemaReason(err))
	}
	cid := e.CorrelationID
	if cid == "" {
		cid = newCorrelationID()
	}
	return p.persist(ctx, event, cid)
}

func (p *Poller) Processors() map[string]dlq.Processor {
	return map[string]dlq.Processor{
		dlq.KindPVMBGReport: p.ProcessPVMBGEntry,
		dlq.KindPVMBGRaw:    p.ProcessPVMBGEntry,
	}
}
