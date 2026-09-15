// Package db owns the SQLite connection and shared persistent state that does
// not belong to a single domain package (decisions, key/value context, event log).
package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite database. It is safe for concurrent use.
type Store struct {
	DB *sql.DB
}

var statements = []string{
	`PRAGMA journal_mode=WAL;`,
	`PRAGMA busy_timeout=5000;`,
	`PRAGMA synchronous=NORMAL;`,

	`CREATE TABLE IF NOT EXISTS projects (
		id         TEXT PRIMARY KEY,
		name       TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL
	);`,

	`CREATE TABLE IF NOT EXISTS agents (
		agent_id     TEXT PRIMARY KEY,
		name         TEXT NOT NULL DEFAULT '',
		type         TEXT NOT NULL DEFAULT 'coding',
		status       TEXT NOT NULL DEFAULT 'offline',
		project_id   TEXT NOT NULL DEFAULT '',
		current_task TEXT NOT NULL DEFAULT '',
		files        TEXT NOT NULL DEFAULT '[]',
		last_msg_id  INTEGER NOT NULL DEFAULT 0,
		last_seen    TEXT NOT NULL,
		created_at   TEXT NOT NULL
	);`,

	`CREATE TABLE IF NOT EXISTS tasks (
		task_id          TEXT PRIMARY KEY,
		project_id       TEXT NOT NULL,
		title            TEXT NOT NULL,
		description      TEXT NOT NULL DEFAULT '',
		status           TEXT NOT NULL DEFAULT 'pending',
		assigned_to      TEXT NOT NULL DEFAULT '',
		created_by       TEXT NOT NULL DEFAULT '',
		progress         INTEGER NOT NULL DEFAULT 0,
		current_activity TEXT NOT NULL DEFAULT '',
		created_at       TEXT NOT NULL,
		updated_at       TEXT NOT NULL
	);`,

	`CREATE TABLE IF NOT EXISTS messages (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		project_id TEXT NOT NULL,
		type       TEXT NOT NULL DEFAULT 'message',
		from_agent TEXT NOT NULL,
		to_agent   TEXT NOT NULL DEFAULT '',
		content    TEXT NOT NULL,
		reply_to   INTEGER NOT NULL DEFAULT 0,
		thread_id  INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL
	);`,

	`CREATE TABLE IF NOT EXISTS decisions (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		project_id TEXT NOT NULL,
		title      TEXT NOT NULL,
		decision   TEXT NOT NULL,
		reason     TEXT NOT NULL DEFAULT '',
		agent_id   TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL
	);`,

	`CREATE TABLE IF NOT EXISTS context (
		project_id TEXT NOT NULL,
		key        TEXT NOT NULL,
		value      TEXT NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		PRIMARY KEY (project_id, key)
	);`,

	`CREATE TABLE IF NOT EXISTS events (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		type       TEXT NOT NULL,
		project_id TEXT NOT NULL DEFAULT '',
		data       TEXT NOT NULL DEFAULT '{}',
		created_at TEXT NOT NULL
	);`,

	`CREATE INDEX IF NOT EXISTS idx_tasks_project ON tasks(project_id);`,
	`CREATE INDEX IF NOT EXISTS idx_tasks_assignee ON tasks(assigned_to);`,
	`CREATE INDEX IF NOT EXISTS idx_messages_project ON messages(project_id, id);`,
	`CREATE INDEX IF NOT EXISTS idx_decisions_project ON decisions(project_id);`,
	`CREATE INDEX IF NOT EXISTS idx_events_project ON events(project_id, id);`,
}

// Open opens (or creates) the SQLite database at path, enables WAL mode and
// applies the schema.
func Open(path string) (*Store, error) {
	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	// SQLite has a single writer; one connection keeps things dead simple and
	// busy_timeout makes concurrent callers wait instead of failing.
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	for _, stmt := range statements {
		if _, err := sqlDB.Exec(stmt); err != nil {
			sqlDB.Close()
			return nil, fmt.Errorf("migrate %q: %w", firstLine(stmt), err)
		}
	}
	return &Store{DB: sqlDB}, nil
}

func (s *Store) Close() error { return s.DB.Close() }

// Now returns the current time as an RFC3339 UTC string, the canonical
// timestamp format used across all tables.
func Now() string { return time.Now().UTC().Format(time.RFC3339) }

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// EnsureProject inserts the project if it does not exist yet.
func (s *Store) EnsureProject(id string) error {
	_, err := s.DB.Exec(
		`INSERT INTO projects (id, name, created_at) VALUES (?, ?, ?)
		 ON CONFLICT(id) DO NOTHING`,
		id, id, Now(),
	)
	return err
}

// ListProjects returns all project ids.
func (s *Store) ListProjects() ([]string, error) {
	rows, err := s.DB.Query(`SELECT id FROM projects ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ---- decisions ----

// Decision is a shared implementation decision recorded by an agent.
type Decision struct {
	ID        int64  `json:"id"`
	ProjectID string `json:"project_id"`
	Title     string `json:"title"`
	Decision  string `json:"decision"`
	Reason    string `json:"reason,omitempty"`
	AgentID   string `json:"agent_id,omitempty"`
	CreatedAt string `json:"created_at"`
}

func (s *Store) RecordDecision(d Decision) (Decision, error) {
	d.CreatedAt = Now()
	res, err := s.DB.Exec(
		`INSERT INTO decisions (project_id, title, decision, reason, agent_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		d.ProjectID, d.Title, d.Decision, d.Reason, d.AgentID, d.CreatedAt,
	)
	if err != nil {
		return d, err
	}
	d.ID, _ = res.LastInsertId()
	return d, nil
}

func (s *Store) ListDecisions(projectID string, limit int) ([]Decision, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.DB.Query(
		`SELECT id, project_id, title, decision, reason, agent_id, created_at
		 FROM decisions WHERE project_id = ? ORDER BY id DESC LIMIT ?`,
		projectID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Decision{}
	for rows.Next() {
		var d Decision
		if err := rows.Scan(&d.ID, &d.ProjectID, &d.Title, &d.Decision, &d.Reason, &d.AgentID, &d.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ---- key/value context ----

// ContextEntry is one key/value pair scoped to a project.
type ContextEntry struct {
	Key       string `json:"key"`
	ProjectID string `json:"project_id"`
	Value     string `json:"value"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func (s *Store) SetContext(projectID, key, value string) error {
	now := Now()
	_, err := s.DB.Exec(
		`INSERT INTO context (project_id, key, value, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(project_id, key) DO UPDATE SET
		   value = excluded.value, updated_at = excluded.updated_at`,
		projectID, key, value, now, now,
	)
	return err
}

// GetContext returns entries for the project. When key is empty all entries
// for the project are returned.
func (s *Store) GetContext(projectID, key string) ([]ContextEntry, error) {
	query := `SELECT project_id, key, value, created_at, updated_at FROM context WHERE project_id = ?`
	args := []any{projectID}
	if key != "" {
		query += ` AND key = ?`
		args = append(args, key)
	}
	query += ` ORDER BY key`
	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ContextEntry{}
	for rows.Next() {
		var e ContextEntry
		if err := rows.Scan(&e.ProjectID, &e.Key, &e.Value, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---- event log (SQLite is the source of truth; see internal/event for the
// in-process fan-out) ----

// EventRow is a persisted event before its data is decoded.
type EventRow struct {
	ID        int64
	Type      string
	ProjectID string
	Data      string
	CreatedAt string
}

func (s *Store) InsertEvent(typ, projectID, dataJSON string) (EventRow, error) {
	row := EventRow{Type: typ, ProjectID: projectID, Data: dataJSON, CreatedAt: Now()}
	res, err := s.DB.Exec(
		`INSERT INTO events (type, project_id, data, created_at) VALUES (?, ?, ?, ?)`,
		row.Type, row.ProjectID, row.Data, row.CreatedAt,
	)
	if err != nil {
		return row, err
	}
	row.ID, _ = res.LastInsertId()
	return row, nil
}

func (s *Store) RecentEvents(projectID string, limit int) ([]EventRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT id, type, project_id, data, created_at FROM events`
	var args []any
	if projectID != "" {
		query += ` WHERE project_id = ? OR project_id = ''`
		args = append(args, projectID)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EventRow{}
	for rows.Next() {
		var r EventRow
		if err := rows.Scan(&r.ID, &r.Type, &r.ProjectID, &r.Data, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	// Reverse so oldest first, matching event order.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, rows.Err()
}

// Counts returns (agents, online agents, tasks, messages, decisions, events).
func (s *Store) Counts() (agents, online, tasks, messages, decisions, events int, err error) {
	queries := []struct {
		q    string
		dest *int
	}{
		{`SELECT COUNT(*) FROM agents`, &agents},
		{`SELECT COUNT(*) FROM agents WHERE status = 'online'`, &online},
		{`SELECT COUNT(*) FROM tasks`, &tasks},
		{`SELECT COUNT(*) FROM messages`, &messages},
		{`SELECT COUNT(*) FROM decisions`, &decisions},
		{`SELECT COUNT(*) FROM events`, &events},
	}
	for _, q := range queries {
		if err = s.DB.QueryRow(q.q).Scan(q.dest); err != nil {
			return
		}
	}
	return
}
