// Package event provides the in-process event hub. Every event is persisted
// to SQLite first (source of truth, offline replay) and then fanned out to
// in-memory subscribers (connected WebSocket clients).
package event

import (
	"encoding/json"
	"log/slog"
	"sync"

	"agentwire/internal/db"
	"agentwire/internal/inbox"
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

// inboxTypes maps hub event types to the semantic inbox event types. Only
// these event types land in agent inboxes (see internal/inbox).
var inboxTypes = map[string]string{
	MessageCreated:      inbox.TypeMessage,
	QuestionCreated:     inbox.TypeQuestion,
	AnswerCreated:       inbox.TypeAnswer,
	InstructionCreated:  inbox.TypeInstruction,
	InterfaceChangeSent: inbox.TypeInterfaceChange,
	TaskProgress:        inbox.TypeProgress,
	TaskCompleted:       inbox.TypeTaskCompleted,
	TaskHandoff:         inbox.TypeTaskHandoff,
	DecisionCreated:     inbox.TypeDecision,
}

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
//
// When an Inbox store is set, Publish also delivers inbox-relevant events
// into the inbox of every agent they concern (durable, ack-based delivery —
// see internal/inbox). The inbox store is optional (nil = no inbox delivery).
type Hub struct {
	DB    *db.Store
	Inbox *inbox.Store

	mu   sync.RWMutex
	subs map[int64]chan Event
	next int64
}

func NewHub(database *db.Store) *Hub {
	return &Hub{DB: database, subs: map[int64]chan Event{}}
}

// SetInbox enables inbox delivery for inbox-relevant events.
func (h *Hub) SetInbox(store *inbox.Store) { h.Inbox = store }

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

	// Inbox delivery happens before fan-out: when a subscriber receives the
	// event, its inbox row already exists and wait_for_events callers are
	// already being woken.
	if semantic, ok := inboxTypes[typ]; ok && h.Inbox != nil {
		h.deliverToInbox(semantic, projectID, row.ID, raw, data)
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

// deliverToInbox inserts one inbox row per agent the event concerns.
func (h *Hub) deliverToInbox(typ, projectID string, eventID int64, raw string, data map[string]any) {
	recipients, err := h.inboxRecipients(typ, projectID, data)
	if err != nil {
		slog.Error("inbox recipient resolution failed", "type", typ, "err", err)
		return
	}
	if len(recipients) == 0 {
		return
	}
	if err := h.Inbox.Add(eventID, typ, projectID, raw, recipients); err != nil {
		slog.Error("inbox insert failed", "type", typ, "err", err)
	}
}

// inboxRecipients computes which agents get an inbox row. Message-type
// events go to the addressed agent (a broadcast goes to everyone in the
// project). Progress, task and decision events go to everyone in the project
// except the originator, who produced the event and already knows it.
func (h *Hub) inboxRecipients(typ, projectID string, data map[string]any) ([]string, error) {
	if inbox.IsMessageType(typ) {
		if to, _ := data["to_agent"].(string); to != "" {
			return []string{to}, nil
		}
		return h.Inbox.ListAgents(projectID)
	}
	originator, _ := data["agent"].(string)
	if originator == "" {
		originator, _ = data["assigned_to"].(string)
	}
	agents, err := h.Inbox.ListAgents(projectID)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(agents))
	for _, a := range agents {
		if a == originator {
			continue
		}
		out = append(out, a)
	}
	return out, nil
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
