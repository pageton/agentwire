package agent

import (
	"path/filepath"
	"testing"

	"agentwire/internal/db"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	database, err := db.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	return NewStore(database)
}

func TestRegisterUpsertPreservesPresence(t *testing.T) {
	s := newStore(t)

	if err := s.Register(Agent{AgentID: "a1", Name: "Agent 1", Status: StatusOnline, ProjectID: "p"}); err != nil {
		t.Fatal(err)
	}
	// Re-register with a different name: identity metadata updates,
	// presence and cursor must survive (identity survives reconnects).
	if err := s.Register(Agent{AgentID: "a1", Name: "Agent One", Status: StatusOffline, ProjectID: "p"}); err != nil {
		t.Fatal(err)
	}
	a, err := s.Get("a1")
	if err != nil || a == nil {
		t.Fatalf("get: %v %v", a, err)
	}
	if a.Name != "Agent One" {
		t.Fatalf("name not updated: %s", a.Name)
	}
	if a.Status != StatusOnline {
		t.Fatalf("presence was clobbered: %s", a.Status)
	}
}

func TestPresence(t *testing.T) {
	s := newStore(t)
	if err := s.Register(Agent{AgentID: "a1", Name: "x", Status: StatusOffline}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPresence("a1", StatusOnline); err != nil {
		t.Fatal(err)
	}
	a, _ := s.Get("a1")
	if a.Status != StatusOnline {
		t.Fatalf("state wrong: %+v", a)
	}
	if a.LastSeen == "" {
		t.Fatal("last_seen missing")
	}
}

func TestFileOverlaps(t *testing.T) {
	s := newStore(t)
	for _, id := range []string{"a1", "a2", "a3"} {
		if err := s.Register(Agent{AgentID: id, Name: id, ProjectID: "p"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SetFiles("a1", []string{"src/a.rs", "src/shared.rs"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetFiles("a2", []string{"src/b.rs", "src/shared.rs"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetFiles("a3", []string{"src/c.rs"}); err != nil {
		t.Fatal(err)
	}
	overlaps, err := s.Overlaps("p")
	if err != nil {
		t.Fatal(err)
	}
	if len(overlaps) != 1 || overlaps[0].File != "src/shared.rs" {
		t.Fatalf("overlaps: %+v", overlaps)
	}
	if len(overlaps[0].Agents) != 2 {
		t.Fatalf("agents: %+v", overlaps[0].Agents)
	}
}
