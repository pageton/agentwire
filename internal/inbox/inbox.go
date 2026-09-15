// Package inbox implements the persistent per-agent inbox: every event that
// concerns an agent is stored as an inbox row keyed by the global event id
// and stays unacknowledged in SQLite until the agent acks it. Delivery is
// at-least-once: real-time push over WebSocket, offline replay on reconnect,
// and an MCP long-poll (wait_for_events) for agents without a socket.
package inbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"time"

	"agentwire/internal/db"
)

// Inbox event types (semantic, stable — independent of the hub event names).
const (
	TypeMessage         = "message"
	TypeQuestion        = "question"
	TypeAnswer          = "answer"
	TypeInstruction     = "instruction"
	TypeProgress        = "progress"
	TypeInterfaceChange = "interface_change"
	TypeDecision        = "decision"
	TypeTaskCompleted   = "task_completed"
	TypeTaskHandoff     = "task_handoff"
)

// messageTypes are the inbox types that originate from agent messages (the
// messages table) rather than task/decision activity.
var messageTypes = map[string]bool{
	TypeMessage:         true,
	TypeQuestion:        true,
	TypeAnswer:          true,
	TypeInstruction:     true,
	TypeInterfaceChange: true,
}

// IsMessageType reports whether the inbox type is a message-type event.
func IsMessageType(typ string) bool { return messageTypes[typ] }

// Event is one inbox entry: an event that concerns this agent. EventID is
// the global event id and the acknowledgement key.
type Event struct {
	EventID   int64          `json:"id"`
	Type      string         `json:"type"`
	ProjectID string         `json:"project_id,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
	Acked     bool           `json:"acked"`
	CreatedAt string         `json:"created_at"`
}

// Store persists inbox rows and wakes wait_for_events callers when new rows
// arrive. One row per (event, agent): the global event is inserted into the
// inbox of every agent it concerns.
type Store struct {
	DB *db.Store

	mu      sync.Mutex
	waiters map[string][]chan struct{}
}

func NewStore(database *db.Store) *Store {
	return &Store{DB: database, waiters: map[string][]chan struct{}{}}
}

// Add inserts one unacknowledged inbox row per recipient and wakes any
// waiters registered for those agents. dataJSON is the event payload as
// persisted in the events table.
func (s *Store) Add(eventID int64, typ, projectID, dataJSON string, agents []string) error {
	now := db.Now()
	for _, agentID := range agents {
		if agentID == "" {
			continue
		}
		if _, err := s.DB.DB.Exec(
			`INSERT OR IGNORE INTO inbox (event_id, agent_id, type, project_id, data, acked, created_at)
			 VALUES (?, ?, ?, ?, ?, 0, ?)`,
			eventID, agentID, typ, projectID, dataJSON, now,
		); err != nil {
			return err
		}
		s.notify(agentID)
	}
	return nil
}

// Unacked returns the agent's unacknowledged inbox rows, oldest first.
func (s *Store) Unacked(agentID string, limit int) ([]Event, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.DB.DB.Query(
		`SELECT event_id, type, project_id, data, acked, created_at
		 FROM inbox WHERE agent_id = ? AND acked = 0
		 ORDER BY event_id ASC LIMIT ?`,
		agentID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

// UnackedCount returns the number of unacknowledged inbox rows for an agent.
func (s *Store) UnackedCount(agentID string) (int, error) {
	var n int
	err := s.DB.DB.QueryRow(
		`SELECT COUNT(*) FROM inbox WHERE agent_id = ? AND acked = 0`, agentID,
	).Scan(&n)
	return n, err
}

// AckThrough acknowledges the event with the given id and everything older
// for the agent. Returns the number of rows acknowledged.
func (s *Store) AckThrough(agentID string, eventID int64) (int64, error) {
	res, err := s.DB.DB.Exec(
		`UPDATE inbox SET acked = 1 WHERE agent_id = ? AND event_id <= ? AND acked = 0`,
		agentID, eventID,
	)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// AckAll acknowledges every unacknowledged inbox row for the agent.
func (s *Store) AckAll(agentID string) (int64, error) {
	res, err := s.DB.DB.Exec(
		`UPDATE inbox SET acked = 1 WHERE agent_id = ? AND acked = 0`,
		agentID,
	)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ListAgents returns the agent ids of a project — the recipient set for
// broadcast and originator-filtered delivery.
func (s *Store) ListAgents(projectID string) ([]string, error) {
	rows, err := s.DB.DB.Query(`SELECT agent_id FROM agents WHERE project_id = ?`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Wait blocks until the agent has unacknowledged inbox rows, until timeout
// elapses, or until ctx is done — whichever comes first — and returns the
// rows (possibly empty on timeout/cancel). Rows pending at entry are
// returned immediately. Event-driven, no DB polling.
func (s *Store) Wait(ctx context.Context, agentID string, timeout time.Duration) ([]Event, error) {
	if rows, err := s.Unacked(agentID, 50); err != nil || len(rows) > 0 {
		return rows, err
	}

	ch := make(chan struct{})
	s.mu.Lock()
	s.waiters[agentID] = append(s.waiters[agentID], ch)
	s.mu.Unlock()
	defer s.unregister(agentID, ch)

	// Re-check after registering: anything inserted before registration is
	// seen here, anything after is seen via the closed channel.
	if rows, err := s.Unacked(agentID, 50); err != nil || len(rows) > 0 {
		return rows, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
	case <-ctx.Done():
	}
	return s.Unacked(agentID, 50)
}

// notify wakes all waiters registered for an agent. Only called under mu
// right after an insert, so no waiter can be woken without rows existing.
func (s *Store) notify(agentID string) {
	s.mu.Lock()
	for _, ch := range s.waiters[agentID] {
		close(ch)
	}
	delete(s.waiters, agentID)
	s.mu.Unlock()
}

func (s *Store) unregister(agentID string, ch chan struct{}) {
	s.mu.Lock()
	list := s.waiters[agentID]
	for i, c := range list {
		if c == ch {
			s.waiters[agentID] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(s.waiters[agentID]) == 0 {
		delete(s.waiters, agentID)
	}
	s.mu.Unlock()
}

func scanEvents(rows *sql.Rows) ([]Event, error) {
	out := []Event{}
	for rows.Next() {
		var e Event
		var data string
		if err := rows.Scan(&e.EventID, &e.Type, &e.ProjectID, &data, &e.Acked, &e.CreatedAt); err != nil {
			return nil, err
		}
		if data != "" {
			_ = json.Unmarshal([]byte(data), &e.Data)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// DataOf returns the value of a string key in an inbox event's data, or "".
func DataOf(e Event, key string) string {
	v, _ := e.Data[key].(string)
	return v
}
