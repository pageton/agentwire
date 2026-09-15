package event

import (
	"path/filepath"
	"testing"
	"time"

	"agentwire/internal/db"
)

func newHub(t *testing.T) *Hub {
	t.Helper()
	database, err := db.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	return NewHub(database)
}

func TestPublishPersistsAndFansOut(t *testing.T) {
	h := newHub(t)

	ch, cancel := h.Subscribe()
	defer cancel()

	ev := h.Publish(TaskProgress, "p", map[string]any{"task": "M.3.7.1", "progress": 70})
	if ev.ID == 0 || ev.CreatedAt == "" {
		t.Fatalf("bad event: %+v", ev)
	}

	select {
	case got := <-ch:
		if got.Type != TaskProgress || got.ProjectID != "p" {
			t.Fatalf("wrong event: %+v", got)
		}
		if got.Data["task"] != "M.3.7.1" {
			t.Fatalf("wrong data: %+v", got.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not receive event")
	}

	recent, err := h.Recent("p", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 1 || recent[0].ID != ev.ID {
		t.Fatalf("persistence mismatch: %+v", recent)
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	h := newHub(t)
	ch, cancel := h.Subscribe()
	cancel()
	h.Publish(MessageCreated, "p", nil)

	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("received event after unsubscribe: %+v", ev)
		}
	default:
	}
}

func TestSlowSubscriberIsDropped(t *testing.T) {
	h := newHub(t)
	// A subscriber that never reads: publish more than the buffer size.
	_, cancel := h.Subscribe()
	defer cancel()

	for i := 0; i < 300; i++ {
		h.Publish(MessageCreated, "p", map[string]any{"i": i})
	}
	// All events must still be persisted, even though the fan-out to the
	// never-reading subscriber dropped messages.
	recent, err := h.Recent("p", 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 300 {
		t.Fatalf("expected 300 persisted events, got %d", len(recent))
	}
}
