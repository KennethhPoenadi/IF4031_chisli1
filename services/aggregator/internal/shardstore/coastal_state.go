package shardstore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"aggregator/internal/coastalstate"
)

type coastalCursorResult struct {
	unit   string
	cursor coastalstate.Cursor
	err    error
}

func (r *Router) LoadCursor(ctx context.Context) (string, error) {
	records, err := r.loadCoastalCursors(ctx)
	if err != nil {
		return "", err
	}
	selected := records[0].cursor
	for _, record := range records[1:] {
		candidate := record.cursor
		if candidate.Revision < selected.Revision {
			selected = candidate
			continue
		}
		if candidate.Revision == selected.Revision && candidate.Value != selected.Value {
			return "", fmt.Errorf("coastal cursor conflict at revision %d", selected.Revision)
		}
	}
	return selected.Value, nil
}

func (r *Router) SaveCursor(ctx context.Context, value string) error {
	r.coastalMu.Lock()
	defer r.coastalMu.Unlock()

	records, err := r.loadCoastalCursors(ctx)
	if err != nil {
		return err
	}
	var maxRevision int64
	for _, record := range records {
		if record.cursor.Revision > maxRevision {
			maxRevision = record.cursor.Revision
		}
	}
	cursor := coastalstate.Cursor{Value: value, Revision: maxRevision + 1, UpdatedAt: time.Now().UTC()}
	failed, written, err := r.writeCoastal(ctx, func(uctx context.Context, unit coastalstate.Unit) error {
		return unit.SaveCoastalCursor(uctx, cursor)
	})
	if err != nil {
		return err
	}
	if len(failed) > 0 {
		r.cfg.Log.Warn("coastal cursor hanya tersimpan sebagian", "revision", cursor.Revision,
			"written", written, "failed_units", failed)
	}
	return nil
}

func (r *Router) Quarantine(ctx context.Context, rejection coastalstate.Rejection) error {
	failed, written, err := r.writeCoastal(ctx, func(uctx context.Context, unit coastalstate.Unit) error {
		return unit.QuarantineCoastal(uctx, rejection)
	})
	if err != nil {
		return err
	}
	if len(failed) > 0 {
		r.cfg.Log.Warn("coastal rejection hanya tersimpan sebagian", "written", written,
			"failed_units", failed, "correlation_id", rejection.CorrelationID)
	}
	return nil
}

func (r *Router) loadCoastalCursors(ctx context.Context) ([]coastalCursorResult, error) {
	results := make(chan coastalCursorResult, len(r.units))
	var wg sync.WaitGroup
	for name, rawUnit := range r.units {
		if r.isDown(name) {
			continue
		}
		unit, ok := rawUnit.(coastalstate.Unit)
		if !ok {
			return nil, fmt.Errorf("store unit %s does not support coastal state", name)
		}
		wg.Add(1)
		go func(name string, unit coastalstate.Unit) {
			defer wg.Done()
			uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
			defer cancel()
			cursor, err := unit.LoadCoastalCursor(uctx)
			if err != nil {
				r.markDown(name, err)
			}
			results <- coastalCursorResult{unit: name, cursor: cursor, err: err}
		}(name, unit)
	}
	wg.Wait()
	close(results)

	var available []coastalCursorResult
	for result := range results {
		if result.err == nil {
			available = append(available, result)
		}
	}
	if len(available) == 0 {
		return nil, fmt.Errorf("%w: coastal state", ErrUnavailable)
	}
	return available, nil
}

func (r *Router) writeCoastal(ctx context.Context, write func(context.Context, coastalstate.Unit) error) ([]string, int, error) {
	failed := make([]string, 0)
	written := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for name, rawUnit := range r.units {
		if r.isDown(name) {
			mu.Lock()
			failed = append(failed, name)
			mu.Unlock()
			continue
		}
		unit, ok := rawUnit.(coastalstate.Unit)
		if !ok {
			return nil, 0, fmt.Errorf("store unit %s does not support coastal state", name)
		}
		wg.Add(1)
		go func(name string, unit coastalstate.Unit) {
			defer wg.Done()
			uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
			defer cancel()
			if err := write(uctx, unit); err != nil {
				r.markDown(name, err)
				mu.Lock()
				failed = append(failed, name)
				mu.Unlock()
				return
			}
			mu.Lock()
			written++
			mu.Unlock()
		}(name, unit)
	}
	wg.Wait()
	if written == 0 {
		return failed, 0, fmt.Errorf("%w: coastal state", ErrUnavailable)
	}
	return failed, written, nil
}

var _ coastalstate.Store = (*Router)(nil)
