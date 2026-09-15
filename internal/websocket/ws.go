// Package websocket implements the real-time event channel: token auth,
// heartbeats, presence, live event push and offline replay on reconnect.
package websocket

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"agentwire/internal/agent"
	"agentwire/internal/db"
	"agentwire/internal/event"
	"agentwire/internal/message"
	"agentwire/internal/task"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 75 * time.Second
	pingPeriod     = 30 * time.Second
	helloTimeout   = 10 * time.Second
	replayLimit    = 200
	eventsLimit    = 100
	decisionsLimit = 20
	maxMessageSize = 256 * 1024
)

// Server handles WebSocket connections.
type Server struct {
	DB     *db.Store
	Hub    *event.Hub
	Agents *agent.Store
	Tasks  *task.Store
	Msgs   *message.Store
	Token  string // empty = auth disabled

	Upgrader websocket.Upgrader

	mu      sync.Mutex
	nextID  int64
	clients map[int64]*client
	byAgent map[string]int64 // agent_id -> active connection id (last wins)
}

func NewServer(database *db.Store, hub *event.Hub, agents *agent.Store, tasks *task.Store, msgs *message.Store, token string) *Server {
	return &Server{
		DB:     database,
		Hub:    hub,
		Agents: agents,
		Tasks:  tasks,
		Msgs:   msgs,
		Token:  token,
		Upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			// Auth is token-based; agent CLI clients may send any Origin.
			CheckOrigin: func(*http.Request) bool { return true },
		},
		clients: map[int64]*client{},
		byAgent: map[string]int64{},
	}
}

// ---- wire protocol ----

type serverEnvelope struct {
	Type       string            `json:"type"`
	Event      *event.Event      `json:"event,omitempty"`
	Messages   []message.Message `json:"messages,omitempty"`
	Events     []event.Event     `json:"events,omitempty"`
	Agents     []agent.Agent     `json:"agents,omitempty"`
	Decisions  []db.Decision     `json:"decisions,omitempty"`
	Error      string            `json:"error,omitempty"`
	AgentID    string            `json:"agent_id,omitempty"`
	ProjectID  string            `json:"project_id,omitempty"`
	ServerTime string            `json:"server_time,omitempty"`
}

type clientEnvelope struct {
	Type    string `json:"type"`
	AgentID string `json:"agent_id,omitempty"`
}

// ServeHTTP upgrades the connection and runs the client session.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Token != "" {
		token := r.URL.Query().Get("token")
		if token == "" {
			token = r.Header.Get("Authorization")
			if len(token) > 7 && token[:7] == "Bearer " {
				token = token[7:]
			}
		}
		if token != s.Token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	conn, err := s.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Debug("ws upgrade failed", "err", err)
		return
	}

	// Wait briefly for a hello message (or fall back to ?agent_id=).
	agentID := r.URL.Query().Get("agent_id")
	conn.SetReadDeadline(time.Now().Add(helloTimeout))
	if agentID == "" {
		var hello clientEnvelope
		if err := conn.ReadJSON(&hello); err == nil && hello.Type == "hello" {
			agentID = hello.AgentID
		}
	}

	c := &client{
		id:      s.newClientID(),
		server:  s,
		conn:    conn,
		send:    make(chan []byte, 256),
		agentID: agentID,
	}
	if agentID == "" {
		// Anonymous listener: gets all events, no presence, no replay.
		slog.Info("ws client connected (anonymous)", "remote", r.RemoteAddr)
	} else {
		if err := s.attach(c); err != nil {
			c.close()
			return
		}
	}

	s.mu.Lock()
	s.clients[c.id] = c
	s.mu.Unlock()

	events, cancel := s.Hub.Subscribe()
	defer cancel()
	c.run(events)
}

func (s *Server) newClientID() int64 { return atomic.AddInt64(&s.nextID, 1) }

// attach registers presence for the agent and replaces any stale connection.
func (s *Server) attach(c *client) error {
	a, err := s.Agents.Get(c.agentID)
	if err != nil {
		slog.Error("agent lookup failed", "agent_id", c.agentID, "err", err)
		return err
	}
	if a == nil {
		// Unknown agent: auto-register a minimal identity so presence and
		// replay work immediately (agents can enrich via register_agent).
		if err := s.Agents.Register(agent.Agent{
			AgentID: c.agentID,
			Name:    c.agentID,
			Status:  agent.StatusOnline,
		}); err != nil {
			return err
		}
		a, _ = s.Agents.Get(c.agentID)
	}
	c.projectID = a.ProjectID

	s.mu.Lock()
	if old, ok := s.byAgent[c.agentID]; ok {
		if oc, ok := s.clients[old]; ok {
			oc.close() // replaced by this newer connection
		}
	}
	s.byAgent[c.agentID] = c.id
	s.mu.Unlock()

	if err := s.Agents.SetPresence(c.agentID, agent.StatusOnline); err != nil {
		slog.Error("presence update failed", "agent_id", c.agentID, "err", err)
	}
	s.Hub.Publish(event.AgentConnected, c.projectID, map[string]any{"agent": c.agentID})
	s.Hub.Publish(event.AgentStatusChange, c.projectID, map[string]any{
		"agent": c.agentID, "status": agent.StatusOnline,
	})
	slog.Info("ws client connected", "agent_id", c.agentID, "project", c.projectID)
	return nil
}

type client struct {
	id        int64
	server    *Server
	conn      *websocket.Conn
	send      chan []byte
	agentID   string
	projectID string
	closed    atomic.Bool
}

func (c *client) close() {
	if c.closed.CompareAndSwap(false, true) {
		c.conn.Close()
	}
}

func (c *client) push(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	select {
	case c.send <- b:
	default:
		// Slow consumer; events are replayable from SQLite.
	}
}

// run drives the writer and event-watcher goroutines and the read loop until
// the connection ends, then cleans up presence.
func (c *client) run(events <-chan event.Event) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go c.writePump(done, &wg)
	go c.watchEvents(events, done, &wg)

	if c.agentID != "" {
		c.sendReplay()
	}

	c.readPump()
	close(done)
	c.close()
	wg.Wait()

	s := c.server
	s.mu.Lock()
	delete(s.clients, c.id)
	if cur, ok := s.byAgent[c.agentID]; ok && cur == c.id {
		delete(s.byAgent, c.agentID)
	}
	s.mu.Unlock()

	if c.agentID != "" {
		if err := s.Agents.SetPresence(c.agentID, agent.StatusOffline); err != nil {
			slog.Error("presence update failed", "agent_id", c.agentID, "err", err)
		}
		s.Hub.Publish(event.AgentDisconnected, c.projectID, map[string]any{"agent": c.agentID})
		s.Hub.Publish(event.AgentStatusChange, c.projectID, map[string]any{
			"agent": c.agentID, "status": agent.StatusOffline,
		})
		slog.Info("ws client disconnected", "agent_id", c.agentID)
	}
}

// watchEvents receives hub events and pushes the ones relevant to this
// client. Messages addressed to this agent also advance the offline delivery
// cursor, since delivery over the socket counts as delivered.
func (c *client) watchEvents(events <-chan event.Event, done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	s := c.server
	for {
		select {
		case <-done:
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			if ev.ProjectID != "" && c.projectID != "" && ev.ProjectID != c.projectID {
				continue // not for my project (anonymous listeners get everything)
			}
			c.push(serverEnvelope{Type: "event", Event: &ev})

			if c.agentID != "" && ev.Type == event.MessageCreated {
				if id, toAgent, proj, ok := messageData(ev.Data); ok &&
					proj == c.projectID && (toAgent == "" || toAgent == c.agentID) {
					if err := s.Agents.AdvanceCursor(c.agentID, id); err != nil {
						slog.Error("cursor advance failed", "agent_id", c.agentID, "err", err)
					}
				}
			}
		}
	}
}

// messageData extracts (id, to_agent, project_id) from a message.created
// event payload. In-process events carry int64 ids; replayed events (JSON
// round-tripped through SQLite) carry float64.
func messageData(data map[string]any) (int64, string, string, bool) {
	to, _ := data["to_agent"].(string)
	proj, _ := data["project_id"].(string)
	switch v := data["id"].(type) {
	case int64:
		return v, to, proj, true
	case float64:
		return int64(v), to, proj, true
	case int:
		return int64(v), to, proj, true
	}
	return 0, to, proj, false
}

// sendReplay delivers everything the agent missed while offline.
func (c *client) sendReplay() {
	s := c.server
	env := serverEnvelope{Type: "replay", AgentID: c.agentID, ProjectID: c.projectID}

	a, err := s.Agents.Get(c.agentID)
	if err != nil || a == nil {
		return
	}

	if msgs, err := s.Msgs.Unread(c.agentID, c.projectID, a.LastMsgID, replayLimit); err == nil && len(msgs) > 0 {
		env.Messages = msgs
		if last := msgs[len(msgs)-1].ID; last > a.LastMsgID {
			if err := s.Agents.AdvanceCursor(c.agentID, last); err != nil {
				slog.Error("cursor advance failed", "agent_id", c.agentID, "err", err)
			}
		}
	}
	if evs, err := s.Hub.Recent(c.projectID, eventsLimit); err == nil {
		env.Events = evs
	}
	if ags, err := s.Agents.List(c.projectID); err == nil {
		env.Agents = ags
	}
	if decs, err := s.DB.ListDecisions(c.projectID, decisionsLimit); err == nil {
		env.Decisions = decs
	}

	// Announce the connection to the client itself.
	c.push(serverEnvelope{Type: "hello", AgentID: c.agentID, ProjectID: c.projectID, ServerTime: db.Now()})
	if len(env.Messages) > 0 || len(env.Events) > 0 {
		c.push(env)
		slog.Info("replayed offline state", "agent_id", c.agentID,
			"messages", len(env.Messages), "events", len(env.Events))
	}
}

func (c *client) writePump(done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case msg := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			// Control-frame ping: gorilla clients and browsers reply
			// automatically, which refreshes the read deadline.
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-done:
			c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return
		}
	}
}

func (c *client) readPump() {
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		var msg clientEnvelope
		if err := c.conn.ReadJSON(&msg); err != nil {
			return
		}
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		switch msg.Type {
		case "ping":
			c.push(serverEnvelope{Type: "pong", ServerTime: db.Now()})
		}
	}
}
