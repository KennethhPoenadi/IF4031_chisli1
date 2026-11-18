package shardstore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"aggregator/internal/pollstate"
)

type pollCursorResult struct {
	unit   string
	cursor pollstate.Cursor
	err    error
}

func (r *Router) LoadPollCursor(ctx context.Context, source string) (string, error) {
	records, err := r.loadPollCursors(ctx, source)
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
			return "", fmt.Errorf("%s poll cursor conflict at revision %d", source, selected.Revision)
		}
	}
	return selected.Value, nil
}

func (r *Router) SavePollCursor(ctx context.Context, source, value string) error {
	source = strings.TrimSpace(source)
	if source == "" {
		return fmt.Errorf("poll cursor source must not be blank")
	}

	r.pollMu.Lock()
	defer r.pollMu.Unlock()

	records, err := r.loadPollCursors(ctx, source)
	if err != nil {
		return err
	}
	var maxRevision int64
	for _, record := range records {
		if record.cursor.Revision > maxRevision {
			maxRevision = record.cursor.Revision
		}
	}
	cursor := pollstate.Cursor{Value: value, Revision: maxRevision + 1, UpdatedAt: time.Now().UTC()}
	failed, written, err := r.writePollState(ctx, func(uctx context.Context, unit pollstate.Unit) error {
		return unit.SavePollCursor(uctx, source, cursor)
	})
	if err != nil {
		return err
	}
	if len(failed) > 0 {
		r.cfg.Log.Warn("poll cursor hanya tersimpan sebagian", "source", source, "revision", cursor.Revision,
			"written", written, "failed_units", failed)
	}
	return nil
}

func (r *Router) loadPollCursors(ctx context.Context, source string) ([]pollCursorResult, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return nil, fmt.Errorf("poll cursor source must not be blank")
	}

	results := make(chan pollCursorResult, len(r.units))
	var wg sync.WaitGroup
	for name, rawUnit := range r.units {
		if r.isDown(name) {
			continue
		}
		unit, ok := rawUnit.(pollstate.Unit)
		if !ok {
			return nil, fmt.Errorf("store unit %s does not support poll state", name)
		}
		wg.Add(1)
		go func(name string, unit pollstate.Unit) {
			defer wg.Done()
			uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
			defer cancel()
			cursor, err := unit.LoadPollCursor(uctx, source)
			if err != nil {
				r.markDownCtx(ctx, name, err)
			}
			results <- pollCursorResult{unit: name, cursor: cursor, err: err}
		}(name, unit)
	}
	wg.Wait()
	close(results)

	var available []pollCursorResult
	for result := range results {
		if result.err == nil {
			available = append(available, result)
		}
	}
	if len(available) == 0 {
		return nil, fmt.Errorf("%w: %s poll state", ErrUnavailable, source)
	}
	return available, nil
}

func (r *Router) writePollState(ctx context.Context, write func(context.Context, pollstate.Unit) error) ([]string, int, error) {
	failed := make([]string, 0)
	written := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for name, rawUnit := range r.units {
		if r.isDown(name) {
			failed = append(failed, name)
			continue
		}
		unit, ok := rawUnit.(pollstate.Unit)
		if !ok {
			return nil, 0, fmt.Errorf("store unit %s does not support poll state", name)
		}
		wg.Add(1)
		go func(name string, unit pollstate.Unit) {
			defer wg.Done()
			uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
			defer cancel()
			if err := write(uctx, unit); err != nil {
				r.markDownCtx(ctx, name, err)
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
		return failed, 0, fmt.Errorf("%w: poll state", ErrUnavailable)
	}
	return failed, written, nil
}

var _ pollstate.Store = (*Router)(nil)
