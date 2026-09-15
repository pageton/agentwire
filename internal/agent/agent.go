// Package agent manages persistent agent identities and presence.
package agent

import (
	"database/sql"
	"encoding/json"
	"errors"

	"agentwire/internal/db"
)

const (
	StatusOnline  = "online"
	StatusOffline = "offline"
)

// Agent is a persistent identity of a coding agent. The identity survives
// reconnects and restarts; the WebSocket connection is temporary.
type Agent struct {
	AgentID     string   `json:"agent_id"`
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Status      string   `json:"status"` // online | offline
	ProjectID   string   `json:"project_id,omitempty"`
	CurrentTask string   `json:"current_task,omitempty"`
	Files       []string `json:"files,omitempty"`
	LastMsgID   int64    `json:"-"` // offline delivery cursor
	LastSeen    string   `json:"last_seen"`
	CreatedAt   string   `json:"created_at"`
}

// Store provides agent persistence on top of the shared database.
type Store struct {
	DB *db.Store
}

func NewStore(database *db.Store) *Store { return &Store{DB: database} }

// Register upserts an agent identity. Presence and delivery cursor are
// preserved across re-registrations.
func (s *Store) Register(a Agent) error {
	if a.Type == "" {
		a.Type = "coding"
	}
	if a.CreatedAt == "" {
		a.CreatedAt = db.Now()
	}
	_, err := s.DB.DB.Exec(
		`INSERT INTO agents (agent_id, name, type, status, project_id, current_task, files, last_seen, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, '[]', ?, ?)
		 ON CONFLICT(agent_id) DO UPDATE SET
		   name = excluded.name,
		   type = excluded.type,
		   project_id = excluded.project_id,
		   last_seen = excluded.last_seen`,
		a.AgentID, a.Name, a.Type, a.Status, a.ProjectID, a.CurrentTask, a.LastSeen, a.CreatedAt,
	)
	return err
}

// Get returns the agent or nil when unknown.
func (s *Store) Get(agentID string) (*Agent, error) {
	row := s.DB.DB.QueryRow(
		`SELECT agent_id, name, type, status, project_id, current_task, files, last_msg_id, last_seen, created_at
		 FROM agents WHERE agent_id = ?`,
		agentID,
	)
	a, err := scanAgent(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return a, err
}

// List returns all agents, optionally filtered by project.
func (s *Store) List(projectID string) ([]Agent, error) {
	query := `SELECT agent_id, name, type, status, project_id, current_task, files, last_msg_id, last_seen, created_at
	          FROM agents`
	var args []any
	if projectID != "" {
		query += ` WHERE project_id = ?`
		args = append(args, projectID)
	}
	query += ` ORDER BY agent_id`
	rows, err := s.DB.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Agent{}
	for rows.Next() {
		a, err := scanAgent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

// SetPresence updates status and last_seen.
func (s *Store) SetPresence(agentID, status string) error {
	_, err := s.DB.DB.Exec(
		`UPDATE agents SET status = ?, last_seen = ? WHERE agent_id = ?`,
		status, db.Now(), agentID,
	)
	return err
}

// SetCurrentTask records which task the agent is working on right now.
func (s *Store) SetCurrentTask(agentID, taskID string) error {
	_, err := s.DB.DB.Exec(
		`UPDATE agents SET current_task = ?, last_seen = ? WHERE agent_id = ?`,
		taskID, db.Now(), agentID,
	)
	return err
}

// SetFiles records the files the agent is currently modifying (JSON array).
func (s *Store) SetFiles(agentID string, files []string) error {
	b, err := json.Marshal(files)
	if err != nil {
		return err
	}
	_, err = s.DB.DB.Exec(
		`UPDATE agents SET files = ?, last_seen = ? WHERE agent_id = ?`,
		string(b), db.Now(), agentID,
	)
	return err
}

// AdvanceCursor moves the agent's offline delivery cursor forward to at
// least msgID. Messages with id > cursor are candidates for offline replay.
func (s *Store) AdvanceCursor(agentID string, msgID int64) error {
	_, err := s.DB.DB.Exec(
		`UPDATE agents SET last_msg_id = MAX(last_msg_id, ?) WHERE agent_id = ?`,
		msgID, agentID,
	)
	return err
}

// SetCursor sets the delivery cursor explicitly (mark_read).
func (s *Store) SetCursor(agentID string, msgID int64) error {
	_, err := s.DB.DB.Exec(
		`UPDATE agents SET last_msg_id = ? WHERE agent_id = ?`,
		msgID, agentID,
	)
	return err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanAgent(row rowScanner) (*Agent, error) {
	var a Agent
	var files string
	err := row.Scan(&a.AgentID, &a.Name, &a.Type, &a.Status, &a.ProjectID, &a.CurrentTask, &files, &a.LastMsgID, &a.LastSeen, &a.CreatedAt)
	if err != nil {
		return nil, err
	}
	if files != "" {
		_ = json.Unmarshal([]byte(files), &a.Files)
	}
	return &a, nil
}

// FileOverlap describes a file modified by more than one agent in a project.
type FileOverlap struct {
	File   string   `json:"file"`
	Agents []string `json:"agents"`
}

// Overlaps computes which files are touched by more than one agent in the
// given project. Purely informational — no locking or merging is attempted.
func (s *Store) Overlaps(projectID string) ([]FileOverlap, error) {
	agents, err := s.List(projectID)
	if err != nil {
		return nil, err
	}
	owners := map[string][]string{}
	for _, a := range agents {
		for _, f := range a.Files {
			if f == "" {
				continue
			}
			owners[f] = append(owners[f], a.AgentID)
		}
	}
	out := []FileOverlap{}
	for f, ids := range owners {
		if len(ids) > 1 {
			out = append(out, FileOverlap{File: f, Agents: ids})
		}
	}
	return out, nil
}
