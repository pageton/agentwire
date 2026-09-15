package event

import (
	"path/filepath"
	"testing"
	"time"

	"agentwire/internal/agent"
	"agentwire/internal/db"
	"agentwire/internal/inbox"
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

// newHubWithInbox returns a hub with inbox delivery enabled and three agents
// registered in project "p".
func newHubWithInbox(t *testing.T) (*Hub, *inbox.Store) {
	t.Helper()
	database, err := db.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	agents := agent.NewStore(database)
	for _, id := range []string{"a1", "a2", "a3"} {
		if err := agents.Register(agent.Agent{AgentID: id, Name: id, ProjectID: "p"}); err != nil {
			t.Fatal(err)
		}
	}
	inbx := inbox.NewStore(database)
	h := NewHub(database)
	h.SetInbox(inbx)
	return h, inbx
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

func TestInboxDeliveryBroadcast(t *testing.T) {
	h, inbx := newHubWithInbox(t)
	h.Publish(MessageCreated, "p", map[string]any{
		"id": 1, "project_id": "p", "type": "message",
		"from_agent": "a1", "to_agent": "", "content": "hello all",
	})
	// Broadcast: every agent in the project gets an inbox row (including
	// the sender — symmetric with the message semantics).
	for _, id := range []string{"a1", "a2", "a3"} {
		rows, err := inbx.Unacked(id, 10)
		if err != nil || len(rows) != 1 || rows[0].Type != inbox.TypeMessage {
			t.Fatalf("agent %s inbox = %+v, %v", id, rows, err)
		}
	}
}

func TestInboxDeliveryDirect(t *testing.T) {
	h, inbx := newHubWithInbox(t)
	h.Publish(QuestionCreated, "p", map[string]any{
		"id": 1, "project_id": "p", "from_agent": "a1",
		"to_agent": "a2", "content": "q?",
	})
	if rows, _ := inbx.Unacked("a2", 10); len(rows) != 1 || rows[0].Type != inbox.TypeQuestion {
		t.Fatalf("a2 inbox = %+v", rows)
	}
	for _, id := range []string{"a1", "a3"} {
		if rows, _ := inbx.Unacked(id, 10); len(rows) != 0 {
			t.Fatalf("agent %s should not get a direct message row: %+v", id, rows)
		}
	}
}

func TestInboxDeliveryExcludesOriginator(t *testing.T) {
	h, inbx := newHubWithInbox(t)
	// a1 publishes progress on its own task: everyone except a1 gets a row.
	h.Publish(TaskProgress, "p", map[string]any{
		"task_id": "T.1", "agent": "a1", "progress": 70,
	})
	if rows, _ := inbx.Unacked("a1", 10); len(rows) != 0 {
		t.Fatalf("originator got its own progress row: %+v", rows)
	}
	for _, id := range []string{"a2", "a3"} {
		if rows, _ := inbx.Unacked(id, 10); len(rows) != 1 || rows[0].Type != inbox.TypeProgress {
			t.Fatalf("agent %s inbox = %+v", id, rows)
		}
	}
}

func TestInboxSkipsNonInboxEvents(t *testing.T) {
	h, inbx := newHubWithInbox(t)
	h.Publish(TaskClaimed, "p", map[string]any{"task_id": "T.1", "agent": "a1"})
	h.Publish(AgentConnected, "p", map[string]any{"agent": "a1"})
	for _, id := range []string{"a1", "a2", "a3"} {
		if n, _ := inbx.UnackedCount(id); n != 0 {
			t.Fatalf("agent %s got rows for non-inbox events: %d", id, n)
		}
	}
}

func TestInboxNilStoreSkipsDelivery(t *testing.T) {
	h := newHub(t) // no inbox store set
	ev := h.Publish(MessageCreated, "p", map[string]any{"to_agent": "a1"})
	if ev.ID == 0 {
		t.Fatal("event not persisted")
	}
}
