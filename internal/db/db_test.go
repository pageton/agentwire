package db

import (
	"path/filepath"
	"testing"
)

func TestOpenAndSchema(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.EnsureProject("td-rs"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureProject("td-rs"); err != nil {
		t.Fatal(err) // idempotent
	}
	projects, err := s.ListProjects()
	if err != nil {
		t.Fatal(err)
	}
	if len(projects) != 1 || projects[0] != "td-rs" {
		t.Fatalf("unexpected projects: %v", projects)
	}
}

func TestContextUpsert(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.SetContext("p", "k", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetContext("p", "k", "v2"); err != nil {
		t.Fatal(err)
	}
	entries, err := s.GetContext("p", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Value != "v2" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
	if entries[0].UpdatedAt == "" || entries[0].CreatedAt == "" {
		t.Fatal("timestamps missing")
	}
}

func TestDecisions(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	d, err := s.RecordDecision(Decision{ProjectID: "p", Title: "t", Decision: "d", Reason: "r", AgentID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if d.ID == 0 {
		t.Fatal("decision id not assigned")
	}
	all, err := s.ListDecisions("p", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].Title != "t" {
		t.Fatalf("unexpected decisions: %+v", all)
	}
	other, err := s.ListDecisions("other", 10)
	if err != nil || len(other) != 0 {
		t.Fatalf("project isolation broken: %v %v", other, err)
	}
}

func TestEventsPersist(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	row, err := s.InsertEvent("task.progress", "p", `{"task":"M.3.7.1"}`)
	if err != nil {
		t.Fatal(err)
	}
	if row.ID == 0 || row.CreatedAt == "" {
		t.Fatalf("bad row: %+v", row)
	}
	recent, err := s.RecentEvents("p", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 1 || recent[0].Type != "task.progress" {
		t.Fatalf("unexpected events: %+v", recent)
	}
}
