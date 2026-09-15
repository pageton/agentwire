package db

import (
	"database/sql"
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

func TestContextSearch(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	seed := map[string]string{
		"api/clock":        "injected for deterministic tests",
		"api/fetch_config": "boolean action on the loop",
		"build/flags":      "-race -count=1",
		"odd":              "100% done",
	}
	for k, v := range seed {
		if err := s.SetContext("p", k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SetContext("other", "api/clock", "different project"); err != nil {
		t.Fatal(err)
	}

	// Prefix filter.
	got, err := s.SearchContext("p", "api/", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Key != "api/clock" || got[1].Key != "api/fetch_config" {
		t.Fatalf("prefix filter: %+v", got)
	}

	// Substring over keys and values.
	got, err = s.SearchContext("p", "", "deterministic", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "api/clock" {
		t.Fatalf("value search: %+v", got)
	}

	// Prefix + search combine with AND.
	got, err = s.SearchContext("p", "api/", "fetch", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "api/fetch_config" {
		t.Fatalf("combined filter: %+v", got)
	}

	// LIKE wildcards in the search term must be matched literally: "%"
	// must only hit the key/value actually containing a percent sign,
	// not act as a wildcard over everything.
	got, err = s.SearchContext("p", "", "%", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "odd" {
		t.Fatalf("wildcard escaping broken: %+v", got)
	}

	// Project isolation.
	got, err = s.SearchContext("other", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Value != "different project" {
		t.Fatalf("project isolation broken: %+v", got)
	}
}

func TestDecisionSupersession(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	d1, err := s.RecordDecision(Decision{ProjectID: "p", Title: "v1", Decision: "clock via global"})
	if err != nil {
		t.Fatal(err)
	}
	d2, err := s.RecordDecision(Decision{ProjectID: "p", Title: "v2", Decision: "clock injected", Supersedes: d1.ID})
	if err != nil {
		t.Fatal(err)
	}
	d3, err := s.RecordDecision(Decision{ProjectID: "p", Title: "v3", Decision: "clock+config injected", Supersedes: d2.ID})
	if err != nil {
		t.Fatal(err)
	}

	decs, err := s.ListDecisions("p", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(decs) != 3 {
		t.Fatalf("unexpected decisions: %+v", decs)
	}
	byID := map[int64]Decision{}
	for _, d := range decs {
		byID[d.ID] = d
	}
	// d1 was superseded twice: attribution goes to the newest replacer.
	if byID[d1.ID].SupersededBy != d3.ID {
		t.Fatalf("d1 superseded by #%d, want #%d", byID[d1.ID].SupersededBy, d3.ID)
	}
	if byID[d2.ID].SupersededBy != d3.ID {
		t.Fatalf("d2 superseded by #%d, want #%d", byID[d2.ID].SupersededBy, d3.ID)
	}
	if byID[d3.ID].SupersededBy != 0 || byID[d3.ID].Supersedes != d2.ID {
		t.Fatalf("d3 chain broken: %+v", byID[d3.ID])
	}

	// Unknown supersede target fails.
	if _, err := s.RecordDecision(Decision{ProjectID: "p", Title: "x", Decision: "x", Supersedes: 999}); err == nil {
		t.Fatal("expected error for unknown supersede target")
	}

	// Cross-project supersede fails.
	if _, err := s.RecordDecision(Decision{ProjectID: "q", Title: "x", Decision: "x", Supersedes: d1.ID}); err == nil {
		t.Fatal("expected error for cross-project supersede")
	}
}

func TestEventsSince(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var ids []int64
	for i, typ := range []string{"task.created", "task.claimed", "task.progress"} {
		proj := "p"
		if i == 1 {
			proj = "" // global event, must be included for project queries
		}
		row, err := s.InsertEvent(typ, proj, "{}")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, row.ID)
	}

	got, err := s.EventsSince("p", ids[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Type != "task.claimed" || got[1].Type != "task.progress" {
		t.Fatalf("events since: %+v", got)
	}

	got, err = s.EventsSince("p", ids[2], 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no events after the last id, got %+v", got)
	}

	latest, err := s.LatestEventID("p")
	if err != nil || latest != ids[2] {
		t.Fatalf("latest event id: %d %v, want %d", latest, err, ids[2])
	}
}

// TestMigrationAddsSupersedes opens a database created by the pre-supersession
// schema and verifies the migration adds the missing columns without data
// loss. It covers both migrated tables (decisions.supersedes, tasks.
// completion_summary/handoff).
func TestMigrationAddsSupersedes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.Exec(`CREATE TABLE decisions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		project_id TEXT NOT NULL,
		title TEXT NOT NULL,
		decision TEXT NOT NULL,
		reason TEXT NOT NULL DEFAULT '',
		agent_id TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.Exec(`CREATE TABLE tasks (
		task_id TEXT PRIMARY KEY,
		project_id TEXT NOT NULL,
		title TEXT NOT NULL,
		description TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'pending',
		assigned_to TEXT NOT NULL DEFAULT '',
		created_by TEXT NOT NULL DEFAULT '',
		progress INTEGER NOT NULL DEFAULT 0,
		current_activity TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = legacy.Exec(`INSERT INTO decisions (project_id, title, decision, created_at)
		VALUES ('p', 'old', 'decision body', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err = legacy.Exec(`INSERT INTO tasks (task_id, project_id, title, status, created_at, updated_at)
		VALUES ('T.1', 'p', 'old task', 'completed', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if err = legacy.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	defer s.Close()

	decs, err := s.ListDecisions("p", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(decs) != 1 || decs[0].Title != "old" || decs[0].Supersedes != 0 {
		t.Fatalf("legacy data broken: %+v", decs)
	}

	// The migrated columns must be usable.
	d2, err := s.RecordDecision(Decision{ProjectID: "p", Title: "new", Decision: "body", Supersedes: decs[0].ID})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.ListDecisions("p", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].ID != d2.ID || got[1].SupersededBy != d2.ID {
		t.Fatalf("supersession after migration broken: %+v", got)
	}

	// Legacy task row survives, new columns are usable.
	if _, err := s.DB.Exec(`UPDATE tasks SET completion_summary = 'done', handoff = '{"summary":"done"}'
		WHERE task_id = 'T.1'`); err != nil {
		t.Fatalf("migrated task columns not usable: %v", err)
	}
}
