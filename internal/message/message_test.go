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

func TestSendAndRelevance(t *testing.T) {
	s := newStore(t)

	bcast, err := s.Send(Message{ProjectID: "p", FromAgent: "agent-1", Content: "hello all"})
	if err != nil {
		t.Fatal(err)
	}
	if bcast.ID == 0 {
		t.Fatal("no id")
	}
	if !bcast.Relevant("agent-2", "p") {
		t.Fatal("broadcast should be relevant to every agent in the project")
	}
	if bcast.Relevant("agent-2", "other") {
		t.Fatal("broadcast must not leak across projects")
	}

	direct, err := s.Send(Message{ProjectID: "p", FromAgent: "agent-1", ToAgent: "agent-2", Content: "for you"})
	if err != nil {
		t.Fatal(err)
	}
	if !direct.Relevant("agent-2", "p") {
		t.Fatal("direct message should be relevant to the recipient")
	}
	if direct.Relevant("agent-3", "p") {
		t.Fatal("direct message must not be relevant to others")
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

func TestUnreadDelivery(t *testing.T) {
	s := newStore(t)

	// Messages before the cursor are delivered; after are unread.
	m1, _ := s.Send(Message{ProjectID: "p", FromAgent: "a1", ToAgent: "a2", Content: "one"})
	_ = m1
	m2, _ := s.Send(Message{ProjectID: "p", FromAgent: "a1", Content: "bcast two"})
	_, _ = s.Send(Message{ProjectID: "p", FromAgent: "a3", ToAgent: "a4", Content: "not for you"})

	unread, err := s.Unread("a2", "p", m1.ID, 100)
	if err != nil {
		t.Fatal(err)
	}
	// m2 (broadcast) and m3 (after cursor) — but m3 is not relevant to a2.
	if len(unread) != 1 || unread[0].ID != m2.ID {
		t.Fatalf("unread = %+v", unread)
	}

	// Deliver: cursor advances past everything.
	max, _ := s.MaxID()
	unread, err = s.Unread("a2", "p", max, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(unread) != 0 {
		t.Fatalf("expected no unread after delivery, got %+v", unread)
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
