// Package message implements agent-to-agent messaging, question/answer
// threads, and offline delivery tracking.
package message

import (
	"database/sql"
	"errors"

	"agentwire/internal/db"
)

// Message types.
const (
	TypeMessage         = "message"
	TypeQuestion        = "question"
	TypeAnswer          = "answer"
	TypeProgress        = "progress"
	TypeInstruction     = "instruction"
	TypeDecision        = "decision"
	TypeWarning         = "warning"
	TypeInterfaceChange = "interface_change"
	TypeBlocked         = "blocked"
	TypeCompleted       = "completed"
)

// Broadcast designates a message addressed to every agent in the project.
const Broadcast = ""

// Message is a persistent message between agents.
// ToAgent == "" means broadcast to the whole project.
type Message struct {
	ID        int64  `json:"id"`
	ProjectID string `json:"project_id"`
	Type      string `json:"type"`
	FromAgent string `json:"from_agent"`
	ToAgent   string `json:"to_agent,omitempty"`
	Content   string `json:"content"`
	ReplyTo   int64  `json:"reply_to,omitempty"`
	ThreadID  int64  `json:"thread_id,omitempty"`
	CreatedAt string `json:"created_at"`
}

// Store provides message persistence.
type Store struct {
	DB *db.Store
}

func NewStore(database *db.Store) *Store { return &Store{DB: database} }

// Send persists a message and returns it with its ID assigned.
func (s *Store) Send(m Message) (*Message, error) {
	if m.Type == "" {
		m.Type = TypeMessage
	}
	m.CreatedAt = db.Now()
	res, err := s.DB.DB.Exec(
		`INSERT INTO messages (project_id, type, from_agent, to_agent, content, reply_to, thread_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ProjectID, m.Type, m.FromAgent, m.ToAgent, m.Content, m.ReplyTo, m.ThreadID, m.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	m.ID, _ = res.LastInsertId()
	return &m, nil
}

// Reply persists an answer to an existing message, inheriting its thread.
// The root of a thread is the id of its first message.
func (s *Store) Reply(parentID int64, m Message) (*Message, error) {
	parent, err := s.Get(parentID)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		return nil, errors.New("message to reply to not found")
	}
	m.ReplyTo = parentID
	m.ThreadID = parent.ThreadID
	if m.ThreadID == 0 {
		m.ThreadID = parentID
	}
	m.ProjectID = parent.ProjectID
	return s.Send(m)
}

// Get returns a message by id or nil when unknown.
func (s *Store) Get(id int64) (*Message, error) {
	row := s.DB.DB.QueryRow(
		`SELECT id, project_id, type, from_agent, to_agent, content, reply_to, thread_id, created_at
		 FROM messages WHERE id = ?`,
		id,
	)
	m, err := scanMessage(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return m, err
}

// List returns recent messages for a project (or across all projects),
// optionally filtered to a specific agent's inbox, oldest first.
func (s *Store) List(projectID, agentID string, limit int) ([]Message, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT id, project_id, type, from_agent, to_agent, content, reply_to, thread_id, created_at
	          FROM messages WHERE 1=1`
	var args []any
	if projectID != "" {
		query += ` AND project_id = ?`
		args = append(args, projectID)
	}
	if agentID != "" {
		query += ` AND (to_agent = '' OR to_agent = ?)`
		args = append(args, agentID)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.DB.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Message{}
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *m)
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanMessage(row rowScanner) (*Message, error) {
	var m Message
	err := row.Scan(&m.ID, &m.ProjectID, &m.Type, &m.FromAgent, &m.ToAgent, &m.Content, &m.ReplyTo, &m.ThreadID, &m.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &m, nil
}
