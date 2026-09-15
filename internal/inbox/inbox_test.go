package inbox

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"agentwire/internal/db"
)

func newStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	database, err := db.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	return NewStore(database), path
}

func add(t *testing.T, s *Store, eventID int64, typ string, agents ...string) {
	t.Helper()
	if err := s.Add(eventID, typ, "p", `{"content":"hello"}`, agents); err != nil {
		t.Fatal(err)
	}
}

func TestAddUnackedAck(t *testing.T) {
	s, _ := newStore(t)
	add(t, s, 10, TypeMessage, "a1", "a2")
	add(t, s, 11, TypeProgress, "a2")

	if n, err := s.UnackedCount("a1"); err != nil || n != 1 {
		t.Fatalf("agent a1 unacked count = %d, %v; want 1", n, err)
	}
	if n, err := s.UnackedCount("a2"); err != nil || n != 2 {
		t.Fatalf("agent a2 unacked count = %d, %v; want 2", n, err)
	}

	rows, err := s.Unacked("a2", 10)
	if err != nil || len(rows) != 2 || rows[0].EventID != 10 || rows[1].EventID != 11 {
		t.Fatalf("a2 unacked = %+v, %v", rows, err)
	}
	if rows[0].Type != TypeMessage || rows[0].Data["content"] != "hello" {
		t.Fatalf("a2 row wrong: %+v", rows[0])
	}

	// Ack through #10: a2's message row goes away, the progress row stays.
	n, err := s.AckThrough("a2", 10)
	if err != nil || n != 1 {
		t.Fatalf("ack = %d, %v; want 1", n, err)
	}
	if rows, _ := s.Unacked("a2", 10); len(rows) != 1 || rows[0].EventID != 11 {
		t.Fatalf("a2 after ack = %+v", rows)
	}
	if rows, _ := s.Unacked("a1", 10); len(rows) != 1 {
		t.Fatalf("a1 lost its row: %+v", rows)
	}

	// AckAll clears everything.
	n, err = s.AckAll("a1")
	if err != nil || n != 1 {
		t.Fatalf("ackall = %d, %v; want 1", n, err)
	}
	if n, _ := s.UnackedCount("a1"); n != 0 {
		t.Fatalf("a1 still has %d unacked", n)
	}
}

func TestUnackedSurvivesReopen(t *testing.T) {
	s, path := newStore(t)
	add(t, s, 10, TypeMessage, "a1")
	if _, err := s.AckThrough("a1", 10); err != nil {
		t.Fatal(err)
	}
	add(t, s, 11, TypeDecision, "a1")
	// Close before reopening (the temp dir lives until the test ends).
	// Reopen on the same file: the unacked row must still be there —
	// reconnect delivers unacknowledged events.
	s.DB.Close()

	database, err := db.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	s2 := NewStore(database)
	rows, err := s2.Unacked("a1", 10)
	if err != nil || len(rows) != 1 || rows[0].EventID != 11 || rows[0].Acked {
		t.Fatalf("after reopen unacked = %+v, %v", rows, err)
	}
}

func TestWaitReturnsImmediatelyWhenPending(t *testing.T) {
	s, _ := newStore(t)
	add(t, s, 10, TypeMessage, "a1")

	rows, err := s.Wait(context.Background(), "a1", time.Second)
	if err != nil || len(rows) != 1 {
		t.Fatalf("wait = %+v, %v", rows, err)
	}
}

func TestWaitWakesOnAdd(t *testing.T) {
	s, _ := newStore(t)

	done := make(chan []Event, 1)
	go func() {
		rows, err := s.Wait(context.Background(), "a1", 5*time.Second)
		if err != nil {
			t.Errorf("wait: %v", err)
			done <- nil
			return
		}
		done <- rows
	}()

	time.Sleep(50 * time.Millisecond) // let the waiter register
	add(t, s, 10, TypeMessage, "a1")

	select {
	case rows := <-done:
		if len(rows) != 1 || rows[0].EventID != 10 {
			t.Fatalf("woken rows = %+v", rows)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not woken by Add")
	}
}

func TestWaitTimesOut(t *testing.T) {
	s, _ := newStore(t)
	start := time.Now()
	rows, err := s.Wait(context.Background(), "a1", 150*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected no rows on timeout, got %+v", rows)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("returned before the timeout")
	}
}

func TestWaitCancelledByContext(t *testing.T) {
	s, _ := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	rows, err := s.Wait(ctx, "a1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected no rows on cancel, got %+v", rows)
	}
}
