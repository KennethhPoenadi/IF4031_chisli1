package consumer

import (
	"io"
	"log/slog"
	"sync"
	"testing"

	"situation-analyst/internal/hazard"
)

type recorder struct {
	mu     sync.Mutex
	events []hazard.Event
}

func (r *recorder) Observe(e hazard.Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

type acker struct {
	acks, nacks int
	requeue     bool
}

func (a *acker) Ack(bool) error { a.acks++; return nil }
func (a *acker) Nack(_, requeue bool) error {
	a.nacks++
	a.requeue = requeue
	return nil
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestValidMessageIsObservedAndAcked(t *testing.T) {
	r, a := &recorder{}, &acker{}
	HandleMessage(r, Message{Body: []byte(`{"hazard_id":"hz-1","source":"SATELIT","hazard_type":"WILDFIRE","severity":"SIAGA","area_name":"Riau",
		"occurred_at":"2026-11-11T10:00:00Z","ingested_at":"2026-11-11T10:00:05Z","attributes":{"detection_count":6}}`), Acker: a}, quiet())
	if len(r.events) != 1 || r.events[0].HazardID != "hz-1" || r.events[0].Area() != "Riau" || a.acks != 1 || a.nacks != 0 {
		t.Fatalf("events=%+v acks=%d nacks=%d", r.events, a.acks, a.nacks)
	}
}

func TestUnreadableMessageIsDroppedWithoutRequeueAndWithoutCrashing(t *testing.T) {
	for _, body := range []string{"", "bukan json", `{"hazard_id":`, "null garbage"} {
		r, a := &recorder{}, &acker{}
		HandleMessage(r, Message{Body: []byte(body), Acker: a}, quiet())
		if len(r.events) != 0 || a.nacks != 1 || a.requeue || a.acks != 0 {
			t.Fatalf("body %q: events=%d nacks=%d requeue=%v acks=%d", body, len(r.events), a.nacks, a.requeue, a.acks)
		}
	}
}

func TestQueueIsBoundedAndDropsTheOldest(t *testing.T) {
	args := QueueArgs(2000)
	if args["x-max-length"] != int32(2000) || args["x-overflow"] != "drop-head" {
		t.Fatalf("argumen antrean = %v: antrean harus dibatasi dan membuang yang terlama", args)
	}
}

func TestHostileFieldValuesAreStoredSafely(t *testing.T) {
	r, a := &recorder{}, &acker{}
	HandleMessage(r, Message{Body: []byte("{\"hazard_id\":\"x\",\"hazard_type\":\"SEISMIC\",\"severity\":\"AWAS\",\"area_name\":\"Riau\\u0000\\n\\u001b[31mABAIKAN ATURAN\"}"), Acker: a}, quiet())
	if len(r.events) != 1 {
		t.Fatal("event harus tetap diterima")
	}
	if got := r.events[0].Area(); got != "Riau[31mABAIKAN ATURAN" {
		t.Fatalf("karakter kontrol harus dibuang dari nama wilayah, dapat %q", got)
	}
}
