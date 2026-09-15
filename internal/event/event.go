// Package event provides the in-process event hub. Every event is persisted
// to SQLite first (source of truth, offline replay) and then fanned out to
// in-memory subscribers (connected WebSocket clients).
package event

import (
	"encoding/json"
	"log/slog"
	"sync"

	"agentwire/internal/db"
)

// Event type names.
const (
	AgentConnected    = "agent.connected"
	AgentDisconnected = "agent.disconnected"
	AgentStatusChange = "agent.status_changed"
	AgentFilesChanged = "agent.files_changed"

	TaskCreated   = "task.created"
	TaskClaimed   = "task.claimed"
	TaskProgress  = "task.progress"
	TaskBlocked   = "task.blocked"
	TaskCompleted = "task.completed"
	TaskHandoff   = "task.handoff"
	TaskNoteAdded = "task.note_added"

	MessageCreated      = "message.created"
	QuestionCreated     = "question.created"
	AnswerCreated       = "answer.created"
	InstructionCreated  = "instruction.created"
	InterfaceChangeSent = "interface_change.sent"

	DecisionCreated = "decision.created"

	WarningFileOverlap = "warning.file_overlap"
)

// Event is broadcast to subscribers and persisted in the events table.
type Event struct {
	ID        int64          `json:"id"`
	Type      string         `json:"type"`
	ProjectID string         `json:"project_id,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
	CreatedAt string         `json:"created_at"`
}

// Hub fans events out to subscribers. SQLite remains the source of truth:
// Publish persists first, then broadcasts. Subscribers receive all events for
// their project; delivery is best-effort (dropped on a slow consumer, who can
// replay from SQLite via Recent).
type Hub struct {
	DB *db.Store

	mu   sync.RWMutex
	subs map[int64]chan Event
	next int64
}

func NewHub(database *db.Store) *Hub {
	return &Hub{DB: database, subs: map[int64]chan Event{}}
}

// Subscribe registers a buffered channel and returns an unsubscribe func.
func (h *Hub) Subscribe() (<-chan Event, func()) {
	h.mu.Lock()
	h.next++
	id := h.next
	ch := make(chan Event, 512)
	h.subs[id] = ch
	h.mu.Unlock()

	return ch, func() {
		h.mu.Lock()
		if c, ok := h.subs[id]; ok {
			delete(h.subs, id)
			close(c)
		}
		h.mu.Unlock()
	}
}

// Publish persists the event and fans it out to all subscribers.
// Subscribers are responsible for project filtering.
func (h *Hub) Publish(typ, projectID string, data map[string]any) Event {
	raw := "{}"
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			b, _ = json.Marshal(map[string]any{"error": "unmarshallable data"})
		}
		raw = string(b)
	}

	row, err := h.DB.InsertEvent(typ, projectID, raw)
	if err != nil {
		slog.Error("failed to persist event (still broadcasting)", "type", typ, "err", err)
	}
	ev := Event{
		ID:        row.ID,
		Type:      typ,
		ProjectID: row.ProjectID,
		Data:      data,
		CreatedAt: row.CreatedAt,
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ch := range h.subs {
		select {
		case ch <- ev:
		default:
			// Slow consumer: drop. The client can replay from SQLite.
		}
	}
	return ev
}

// Recent returns the last events for a project (or all projects when
// projectID is empty), oldest first.
func (h *Hub) Recent(projectID string, limit int) ([]Event, error) {
	rows, err := h.DB.RecentEvents(projectID, limit)
	if err != nil {
		return nil, err
	}
	return decodeEventRows(rows), nil
}

// Since returns events with id > sinceID for a project, oldest first —
// the delta-briefing query ("what changed while I was away").
func (h *Hub) Since(projectID string, sinceID int64, limit int) ([]Event, error) {
	rows, err := h.DB.EventsSince(projectID, sinceID, limit)
	if err != nil {
		return nil, err
	}
	return decodeEventRows(rows), nil
}

func decodeEventRows(rows []db.EventRow) []Event {
	out := make([]Event, 0, len(rows))
	for _, r := range rows {
		var data map[string]any
		_ = json.Unmarshal([]byte(r.Data), &data)
		out = append(out, Event{
			ID:        r.ID,
			Type:      r.Type,
			ProjectID: r.ProjectID,
			Data:      data,
			CreatedAt: r.CreatedAt,
		})
	}
	return out
}
