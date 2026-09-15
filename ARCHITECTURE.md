# AgentWire Architecture

Small, boring, single-process Go. The whole system is:

```
agents (LLMs) ── MCP tools ──► SQLite  ◄── CLI (status/agents/tasks/activity)
       │                          │
       │                     event hub (in-process) ──► inbox (per-agent, ack-based)
       │                          │
       └── WebSocket ── live push / presence / inbox replay
```

## Principles

1. **SQLite is the source of truth.** Every state change lands in SQLite
   first. WAL mode makes multi-process access safe (the CLI reads the same
   file while the server runs).
2. **Agents are the intelligence.** AgentWire never executes code, never
   controls an LLM, never merges files. It only connects, communicates and
   records.
3. **Parallel by default.** Tasks have no execution dependencies. Claiming a
   task never waits. Coordination happens through messages.
4. **Push, not polling.** Live updates travel over WebSocket. The durable
   inbox holds events until the agent *acknowledges* them; reconnecting
   agents get exact replay of everything unacknowledged. WebSocket delivers
   in real time — it never interrupts an LLM generation; the agent's
   adapter/runtime injects incoming events at the next turn and acks after
   the LLM consumed them.

## Components

### `internal/db`

`db.Open(path)` opens SQLite with `journal_mode=WAL`, `busy_timeout=5000`,
single connection (`MaxOpenConns=1` — SQLite has one writer anyway; the busy
timeout turns concurrent writers into waiters). Applies the schema:

```
projects   (id, name, created_at)
agents     (agent_id, name, type, status, project_id, current_task,
            files JSON, last_seen, created_at)
tasks      (task_id, project_id, title, description, status,
            assigned_to, created_by, progress, current_activity,
            completion_summary, handoff JSON, created_at, updated_at)
messages   (id, project_id, type, from_agent, to_agent, content,
            reply_to, thread_id, created_at)
decisions  (id, project_id, title, decision, reason, agent_id,
            supersedes, created_at)
context    (project_id, key, value, created_at, updated_at)
events     (id, type, project_id, data JSON, created_at)
inbox      (event_id, agent_id, type, project_id, data JSON, acked,
            created_at, PK (event_id, agent_id))
task_notes (id, task_id, project_id, agent_id, note, created_at)
```

Timestamps are RFC3339 UTC strings, generated in Go.

Also owns decisions, key/value context, and the event log — everything that
does not belong to a domain package. Column additions that `CREATE TABLE IF
NOT EXISTS` cannot express (e.g. `decisions.supersedes`) are applied by a
small idempotent migration in `Open` (checked via `PRAGMA table_info`).

### `internal/agent`

Persistent identities. `Register` is an upsert that updates identity metadata
(name, type, project) but **never clobbers presence** — the identity must
survive reconnects and restarts.

- `SetPresence(online|offline)` + `last_seen`
- `SetFiles([]string)` — files currently being modified (JSON column)
- `Overlaps(project)` — files touched by more than one agent (warning only)

### `internal/task`

Tasks are informational coordination units:

```
pending → working → completed
              └──► blocked (reversible)
```

- `Claim` sets `working` + assignee. It never waits — parallel work is the
  default and the only mode.
- `UpdateProgress(progress, activity, status?)` clamps 0–100, updates the
  human-readable activity, optionally flips status (e.g. `blocked`).
- `Complete` sets 100% + `completed`.
- `CompleteWithHandoff` sets 100% + `completed` plus the completion summary
  and a structured `Handoff` (summary, changed files, implementation
  details, decisions made, next steps, relevant context keys) stored as JSON
  on the task row. Purely informational: a handoff never gates, unlocks or
  triggers other tasks. Completion publishes `task.completed`; a handoff
  additionally publishes `task.handoff` carrying the full handoff. Handoffs
  surface in `get_task`, briefings ("Recently completed") and replay.
- `AddNote / Notes` — append-only notes attached to a task (the `task_notes`
  table). Durable knowledge that survives message history scrolling away:
  interface facts, blockers, handoff context. Notes require an existing task
  and are published as `task.note_added`; briefings include the notes on the
  agent's current task.

### `internal/message`

- `Send` persists; `ToAgent == ""` means broadcast to the project.
- `Reply(parentID, …)` addresses the original sender and inherits the
  thread. The thread root is the id of the thread's first message
  (`thread_id = parent.thread_id`, or `parent.id` when the parent has no
  thread).
- `List(project, agent, limit)` — recent history, optionally scoped to an
  agent's inbox.

### `internal/inbox`

The durable per-agent inbox — the delivery guarantee of the agent protocol.
One row per (event, agent), stored in SQLite, acknowledged explicitly.

- `Add(eventID, type, project, dataJSON, agents)` — one unacknowledged row
  per recipient, then wakes any `wait_for_events` waiters for them.
- `Unacked(agent, limit)` / `UnackedCount(agent)` — read the inbox, oldest
  first (ordered by global event id).
- `AckThrough(agent, eventID)` / `AckAll(agent)` — acknowledgement; acked
  rows are never re-delivered.
- `Wait(ctx, agent, timeout)` — block until new inbox rows arrive, the
  timeout elapses, or ctx is done. Event-driven (woken by `Add`), no DB
  polling.

Delivery semantics: **at-least-once until acknowledged**. Real-time push
over WebSocket, replay on reconnect, `wait_for_events` for MCP-only agents.
A WebSocket frame can never interrupt an LLM generation — the agent's
adapter/runtime injects inbox events at the next turn and acks after the
LLM consumed them.

### `internal/event`

The in-process hub. `Publish(type, projectID, data)`:

1. inserts the event into SQLite (`events` table) — persistence first,
2. delivers inbox-relevant events into the inbox of every agent they
   concern (message events → the addressed agent or everyone on broadcast;
   progress/task/decision events → everyone in the project except the
   originator),
3. fans out to subscribers (buffered chan, non-blocking send).

A slow subscriber is dropped for real-time purposes but loses nothing — it
can replay from SQLite. Events:

```
agent.connected / agent.disconnected / agent.status_changed
agent.files_changed
TaskCreated   / task.claimed / task.progress / task.blocked / task.completed
task.handoff / task.note_added
message.created / question.created / answer.created
instruction.created / interface_change.sent
decision.created
warning.file_overlap
```

Inbox-relevant events (message/question/answer/instruction/interface_change
sent, task.progress, task.completed, task.handoff, decision.created) carry
the full payload in the inbox row's `data`, so replay and `get_unread` need
no follow-up queries.

### `internal/websocket`

`GET /ws?agent_id=…&token=…` (token optional when `AGENTWIRE_TOKEN` is empty).
Per connection:

- **Auth** — `?token=` or `Authorization: Bearer` (CLI clients have no
  Origin, so `CheckOrigin` allows all; the token is the gate).
- **Hello** — the first JSON message `{"type":"hello","agent_id":…}` (or the
  `agent_id` query param) binds the connection to an identity. Unknown
  agents are auto-registered with a minimal identity so presence and replay
  work immediately.
- **Presence** — binding marks the agent `online` and publishes
  `agent.connected` + `agent.status_changed`. A new connection for the same
  agent replaces the old one (last wins). On close, the agent goes
  `offline` and `agent.disconnected` is published.
- **Replay** — once, right after bind: every unacknowledged inbox event
  for the agent, recent project events, the agent roster, recent decisions.
  Nothing is acknowledged by the replay — it re-delivers.
- **Heartbeat** — control-frame ping every 30 s (browsers and gorilla
  clients answer automatically); read deadline 75 s refreshed by any read
  or pong. App-level `{"type":"ping"}` → `{"type":"pong"}` also supported.
- **Live push** — a hub subscription filters events by project (anonymous
  listeners get everything). Each event is wrapped as
  `{"type":"event","event":{…}}`. Push is real-time transport only: it
  never acks. The client acks with `{"type":"ack","event_id":N}` (omit
  `event_id` to ack everything) once its LLM has processed the events.
- Writers are serialized through a per-client send channel; slow consumers
  drop (replayable), never block the hub.

### `internal/mcp`

27 tools on top of the stores + hub (see README). Transport: **streamable
HTTP** at `/mcp` (modern clients) and **legacy SSE** at `/sse` for older
ones. Identity is an explicit `agent_id` argument (works with stdio-style
clients too); clients that send HTTP headers may use `X-Agent-Id` as a
fallback. Tool results are compact text, formatted for LLM consumption.

Inbox tools (the agent protocol surface):

- **`get_unread`** — the unacknowledged inbox, oldest first.
- **`ack_event`** — acknowledge through an event id, or everything.
- **`wait_for_events`** — event-driven long-poll: blocks until new inbox
  rows arrive or the timeout elapses (default 30 s, max 60 s). The handler
  holds the request context, so a disconnecting client stops the wait.
- `get_messages(unread_only=true)` reads the message-type subset of the
  inbox; `mark_read` is `ack_event` without an event id — one inbox, one
  ack, two reading surfaces.

Knowledge helpers on top of the primitives:

- **Task handoffs.** `complete_task` optionally records a structured
  handoff (summary, changed files, details, decisions made, next steps,
  context keys) together with the completion. `get_task` renders the full
  picture — handoff, completion summary, notes, and the resolved values of
  the handoff's context keys. Briefings list recently completed tasks with
  their summaries. Informational only, parallel work unaffected.
- **Decision supersession.** `record_decision` accepts `supersedes`; the
  target must exist and belong to the same project. `ListDecisions` computes
  `SupersededBy` pointing at the *head* of the replacement chain, so
  briefings and `get_decisions(active_only=true)` show only the decision
  currently in force.
- **Context discovery.** `list_context` filters by key prefix and/or a
  substring of key or value (LIKE wildcards escaped — user input matches
  literally). Knowledge you cannot find does not exist.
- **Delta briefings.** Every briefing ends with the latest event id; passing
  it back as `get_briefing(since_event_id=…)` returns only the events since
  then (plus the agent's current task state and unread count) — the event
  id is a per-project cursor over all state changes.

### `cmd/agentwire`

`start` wires everything into one HTTP server (mux: `/mcp`, `/sse`,
`/message`, `/ws`, `/healthz`), logs with slog, shuts down on SIGINT/SIGTERM.
`status`/`agents`/`tasks`/`activity` open the SQLite file read-only — WAL
makes this safe while the server runs.

## Data flow: a message from Agent 1 to Agent 2

```
Agent 1 → MCP send_message ──► message.Store.Send ──► SQLite (messages)
                                        │
                              event.Hub.Publish("message.created")
                                        │
                     ┌──────────────────┴─────────────────┐
                     │                                    │
              SQLite (events)                    inbox.Add (SQLite inbox rows
                     │                          for every recipient) + wake
                     │                          wait_for_events waiters
                     │                                    │
                     └──────── fan-out to WS subscribers ─┤
                                                          │
                                          Agent 2 online? ──► push event
                                          envelope, live (no ack)
                                          Agent 2 offline ──► the unacked
                                          inbox row waits in SQLite
```

Reconnect of Agent 2:

```
WS connect → bind identity → presence online → replay:
   inbox rows where acked = 0 AND agent = me   (re-delivered, not acked)
   recent events (project)
   agents (roster)
   decisions
→ live subscription active
```

Agent 2's adapter injects the events at the next LLM turn and acks
(`{"type":"ack","event_id":N}` over WS, or the `ack_event` tool):

```
ack → inbox rows acked = 1 → never re-delivered
```

Result: **at-least-once delivery until acknowledged** — no message is ever
lost, and every unacknowledged event is re-delivered on reconnect.

## Progress flow

```
Agent 1 → MCP update_progress ──► tasks row (progress, activity)
                        └─► event.Hub.Publish("task.progress")
                                        │
                              inbox rows for every other agent
                              in the project (originator excluded)
                                        │
                              WS push to all project subscribers:
                              {"type":"event","event":{
                                 "type":"task.progress","project_id":"td-rs",
                                 "data":{"agent":"agent-1","task_id":"M.3.7.1",
                                         "progress":70,"current_activity":"…"}}}
```

## File overlap

`set_files` stores the agent's current file list and publishes
`agent.files_changed`. `Overlaps` then scans the project's agents; every file
owned by more than one agent is reported (tool `get_file_overlaps`, in
`get_project_activity`, in briefings, and as a `warning.file_overlap` event).
AgentWire never locks or merges files — visibility only.

## Why it stays small

- One process, one SQLite file, one HTTP server.
- The "broker" is a `map[string]*client` with a mutex, plus one buffered
  channel per connection.
- Events are persisted and fanned out by the same `Publish` call; replay is
  a plain SQL query; `wait_for_events` is a map of agent id → waiter
  channels, woken by the inbox insert.
- No framework beyond the MCP and WebSocket protocol libraries.
- No Redis, no Kafka, no message queue — SQLite WAL and in-process channels
  cover it.
