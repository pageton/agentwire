package message

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

func TestSendBasics(t *testing.T) {
	s := newStore(t)

	bcast, err := s.Send(Message{ProjectID: "p", FromAgent: "agent-1", Content: "hello all"})
	if err != nil {
		t.Fatal(err)
	}
	if bcast.ID == 0 {
		t.Fatal("no id")
	}
	if bcast.ToAgent != Broadcast {
		t.Fatalf("broadcast should have empty to_agent, got %q", bcast.ToAgent)
	}

	direct, err := s.Send(Message{ProjectID: "p", FromAgent: "agent-1", ToAgent: "agent-2", Content: "for you"})
	if err != nil {
		t.Fatal(err)
	}
	if direct.ToAgent != "agent-2" {
		t.Fatalf("direct message lost its recipient: %q", direct.ToAgent)
	}
	got, err := s.Get(direct.ID)
	if err != nil || got == nil {
		t.Fatalf("get: %v %v", got, err)
	}
	if got.Content != "for you" {
		t.Fatalf("roundtrip mismatch: %q", got.Content)
	}
}

func TestReplyThreading(t *testing.T) {
	s := newStore(t)

	q, err := s.Send(Message{ProjectID: "p", Type: TypeQuestion, FromAgent: "a1", ToAgent: "a2", Content: "q?"})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := s.Reply(q.ID, Message{Type: TypeAnswer, FromAgent: "a2", ToAgent: "a1", Content: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if reply.ReplyTo != q.ID {
		t.Fatalf("reply_to: %d, want %d", reply.ReplyTo, q.ID)
	}
	if reply.ThreadID != q.ID {
		t.Fatalf("thread root should be the question id: %d", reply.ThreadID)
	}
	// Second-level reply stays in the same thread.
	r2, err := s.Reply(reply.ID, Message{Type: TypeAnswer, FromAgent: "a1", ToAgent: "a2", Content: "thanks"})
	if err != nil {
		t.Fatal(err)
	}
	if r2.ThreadID != q.ID || r2.ReplyTo != reply.ID {
		t.Fatalf("nested reply lost thread: %+v", r2)
	}
}

func TestListHistory(t *testing.T) {
	s := newStore(t)
	if _, err := s.Send(Message{ProjectID: "p", FromAgent: "a1", Content: "hi"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Send(Message{ProjectID: "p", FromAgent: "a2", Content: "hey"}); err != nil {
		t.Fatal(err)
	}
	all, err := s.List("p", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].ID >= all[1].ID {
		t.Fatalf("history order wrong: %+v", all)
	}
}
