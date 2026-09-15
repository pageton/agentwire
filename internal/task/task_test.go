package task

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

func TestLifecycle(t *testing.T) {
	s := newStore(t)

	if err := s.Create(Task{TaskID: "M.3.7.1", ProjectID: "td-rs", Title: "expiry logic"}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get("M.3.7.1")
	if err != nil || got == nil {
		t.Fatalf("get: %v %v", got, err)
	}
	if got.Status != StatusPending {
		t.Fatalf("expected pending, got %s", got.Status)
	}

	// Claim: no dependency gating — starts immediately.
	got, err = s.Claim("M.3.7.1", "agent-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusWorking || got.AssignedTo != "agent-1" {
		t.Fatalf("claim failed: %+v", got)
	}

	got, err = s.UpdateProgress("M.3.7.1", 65, "Implementing expiry", "")
	if err != nil {
		t.Fatal(err)
	}
	if got.Progress != 65 || got.CurrentActivity != "Implementing expiry" {
		t.Fatalf("progress failed: %+v", got)
	}

	got, err = s.UpdateProgress("M.3.7.1", 150, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if got.Progress != 100 {
		t.Fatalf("progress not clamped: %d", got.Progress)
	}

	got, err = s.UpdateProgress("M.3.7.1", 40, "", StatusBlocked)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusBlocked {
		t.Fatalf("blocked failed: %+v", got)
	}

	got, err = s.Complete("M.3.7.1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusCompleted || got.Progress != 100 {
		t.Fatalf("complete failed: %+v", got)
	}
}

// Tasks must never block each other: claiming a task while another is
// pending must succeed and both may be working simultaneously.
func TestParallelWork(t *testing.T) {
	s := newStore(t)

	for _, id := range []string{"M.3.7.1", "M.3.7.2", "M.3.7.3"} {
		if err := s.Create(Task{TaskID: id, ProjectID: "td-rs", Title: id}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Claim("M.3.7.2", "agent-2"); err != nil {
		t.Fatal(err) // must not wait for M.3.7.1
	}
	if _, err := s.Claim("M.3.7.1", "agent-1"); err != nil {
		t.Fatal(err)
	}
	working, err := s.List("td-rs", StatusWorking, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(working) != 2 {
		t.Fatalf("expected 2 tasks working in parallel, got %d", len(working))
	}
}

func TestListFilters(t *testing.T) {
	s := newStore(t)
	if err := s.Create(Task{TaskID: "a", ProjectID: "p", Title: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(Task{TaskID: "b", ProjectID: "q", Title: "b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim("b", "agent-9"); err != nil {
		t.Fatal(err)
	}
	byProj, _ := s.List("p", "", "")
	if len(byProj) != 1 || byProj[0].TaskID != "a" {
		t.Fatalf("project filter: %+v", byProj)
	}
	byAgent, _ := s.List("", "", "agent-9")
	if len(byAgent) != 1 || byAgent[0].TaskID != "b" {
		t.Fatalf("assignee filter: %+v", byAgent)
	}
}

func TestInvalidStatus(t *testing.T) {
	s := newStore(t)
	if err := s.Create(Task{TaskID: "x", ProjectID: "p", Title: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateProgress("x", 10, "", "bogus"); err == nil {
		t.Fatal("expected error for invalid status")
	}
}
