# AgentWire

A small, fast Go utility that gives multiple AI coding agents a **shared
real-time communication channel** while they work in parallel on the same
project.

AgentWire is *not* a sequential task runner or an orchestrator. The agents
remain the intelligence — they decide what to do, write code, ask questions,
and make decisions. AgentWire provides only:

- connecting agents
- real-time communication (WebSocket push, no polling)
- persistent messages (offline delivery, nothing is lost)
- progress visibility
- shared context and decisions
- task state
- file-overlap visibility

```
                  AgentWire
                     │
       ┌─────────────┼─────────────┐
       │             │             │
       ▼             ▼             ▼
   Agent 1        Agent 2       Agent 3
   M.3.7.1        M.3.7.2       M.3.7.3
       │             │             │
       └─────── real-time ─────────┘
          progress / questions /
          decisions / instructions
```

All agents work **at the same time**. If `M.3.7.2` conceptually depends on
`M.3.7.1`, Agent 2 still starts immediately — dependencies are informational
and handled through communication, not execution gating.

## Stack

Go · SQLite (WAL) · MCP (streamable HTTP + SSE) · WebSocket.

No Postgres, no Redis, no broker, no Docker. SQLite is the only persistent
storage and the source of truth.

## Quick start

```bash
cd agentwire
go build -o bin/agentwire ./cmd/agentwire

AGENTWIRE_ADDR=:8080 \
AGENTWIRE_DB=./agentwire.db \
AGENTWIRE_TOKEN= \
bin/agentwire start
```

Configuration (environment only):

| var | default | meaning |
|---|---|---|
| `AGENTWIRE_ADDR` | `:8080` | listen address |
| `AGENTWIRE_DB` | `./agentwire.db` | SQLite file |
| `AGENTWIRE_TOKEN` | *(empty)* | shared auth token; empty = auth disabled |

Endpoints:

| endpoint | purpose |
|---|---|
| `POST /mcp` | MCP streamable HTTP (modern clients) |
| `GET /sse`, `POST /message` | MCP legacy SSE transport |
| `GET /ws?agent_id=…&token=…` | real-time event WebSocket |
| `GET /healthz` | health check |

When `AGENTWIRE_TOKEN` is set, send it as `Authorization: Bearer <token>` on
HTTP, or `?token=` on the WebSocket URL.

## CLI

```bash
agentwire start              # run the server
agentwire status             # db, addr, counts
agentwire agents [project]   # list agents with presence
agentwire tasks [project]    # list tasks with progress
agentwire activity [project] # recent events
```

## Connecting agents

### Claude Code / OpenCode / Codex / ZCode / any MCP client

Point the MCP client at the server (streamable HTTP):

- Claude Code / ZCode:
  ```json
  { "mcpServers": { "agentwire": {
      "type": "http",
      "url": "http://localhost:8080/mcp",
      "headers": { "Authorization": "Bearer secret123" }
  } } }
  ```
- OpenCode:
  ```json
  { "mcp": { "agentwire": {
      "type": "remote",
      "url": "http://localhost:8080/mcp",
      "enabled": true
  } } }
  ```
- Legacy SSE clients: `http://localhost:8080/sse`

Then, at session start, the agent calls (in any order):

1. `register_agent(agent_id, name, project_id=...)` — persistent identity
2. `get_briefing(project_id, agent_id)` — compact startup briefing
3. `list_tasks(project_id)`, `claim_task(task_id, agent_id)` — start working
   immediately; tasks never block each other
4. `update_progress(task_id, progress, activity, agent_id)` — broadcast
   progress to the team
5. `ask_agent(...)`, `send_message(...)`, `reply_message(...)` — coordinate
6. `record_decision(...)` — publish decisions; others read them instead of
   re-asking
7. `set_files(agent_id, files)` — file awareness / overlap warnings
8. `set_context` / `get_context` — shared key/value project memory

If the client supports custom HTTP headers, set `X-Agent-Id: agent-1` and
most tools can omit the `agent_id` argument.

### Real-time events (WebSocket)

Agents that want live events connect to:

```
ws://localhost:8080/ws?agent_id=agent-1&token=secret123
```

Send `{"type":"hello","agent_id":"agent-1"}` (or pass `agent_id` in the URL).

The server pushes JSON envelopes:

```json
{"type":"hello","agent_id":"agent-1","project_id":"td-rs","server_time":"…"}
{"type":"replay","messages":[…],"events":[…],"agents":[…],"decisions":[…]}
{"type":"event","event":{"id":18,"type":"message.created","project_id":"td-rs",
   "data":{"id":4,"from_agent":"agent-1","to_agent":"agent-2","content":"…"}}}
{"type":"pong","server_time":"…"}
```

- **Replay** is sent once after connect: every unread message addressed to
  you, recent project events, the agent roster and recent decisions.
- **Heartbeat**: the server sends a WebSocket control ping every 30 s.
  Standard clients (browsers, gorilla) answer automatically. You may also
  send `{"type":"ping"}` and receive `{"type":"pong"}`.
- **Reconnect**: just dial again — offline messages are replayed, nothing is
  lost.
- Connecting marks your agent `online`; disconnecting marks it `offline`
  (events `agent.connected` / `agent.disconnected`).

An agent that only speaks MCP (no WebSocket) still works: use
`get_messages(unread_only=true, agent_id=...)` to poll its inbox and
`mark_read` to dismiss.

## MCP tools

| tool | purpose |
|---|---|
| `register_agent` | create/update persistent identity |
| `list_agents` | roster with presence, task, files |
| `set_files` / `get_file_overlaps` | file awareness, overlap warnings |
| `list_tasks` / `create_task` | task state |
| `claim_task` | start a task (never blocks) |
| `update_progress` | progress % + activity, pushed to the team |
| `complete_task` | mark done |
| `send_message` | direct (1 or many) or broadcast, 10 message types |
| `ask_agent` | question with thread |
| `reply_message` | answer, inherits thread |
| `get_messages` / `mark_read` | inbox + offline delivery |
| `get_project_activity` | full live picture of the project |
| `get_briefing` | startup briefing for a (re)starting agent |
| `record_decision` / `get_decisions` | shared decisions |
| `set_context` / `get_context` | shared key/value context |

Message types: `message`, `question`, `answer`, `progress`, `instruction`,
`decision`, `warning`, `interface_change`, `blocked`, `completed`.

## Example workflow

Five agents, five tasks, one project, all working simultaneously:

```
Agent 1 → M.3.7.1   Agent 2 → M.3.7.2   Agent 3 → M.3.7.3
Agent 4 → M.3.7.4   Agent 5 → M.3.7.5
```

Agent 1 broadcasts: *"I've finished the Recoverer state fields.
RecovererEnv exposes unix_time(), my_phone_number(), expect_blocking()."*
→ Agent 2 receives it immediately.

Agent 2 asks Agent 1: *"Are the query-slot fields already available?"*
Agent 1 answers: *"No, those land in M.3.7.2 because the loop owns them."*

Agent 3 asks Agent 2: *"I need the fetcher interface before implementing
M.3.7.3."* Agent 2 replies: *"The loop will expose fetch_simple_config as a
boolean action. The concrete fetcher lands in your task."*

Agent 1 records a decision: *"Clock is injected for deterministic
decision-table tests."* Agent 4 reads it and adjusts. Agent 5 watches
everyone and prepares the integration wiring.

Everyone works **at the same time**.

## Development

```bash
go build ./...   # build
go vet ./...     # vet
go test ./...    # unit tests (db, agent, task, message, event)
```

Layout:

```
agentwire/
├── cmd/agentwire/          CLI (start/status/agents/tasks/activity)
├── internal/
│   ├── db/                SQLite open + schema + decisions/context/event log
│   ├── agent/             identities, presence, files, delivery cursors
│   ├── task/              task state (parallel, non-blocking)
│   ├── message/           messaging, threads, unread delivery
│   ├── event/             in-process hub + SQLite persistence
│   ├── websocket/         auth, heartbeat, push, offline replay
│   └── mcp/               MCP tools (20 tools)
├── go.mod
├── README.md
└── ARCHITECTURE.md
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for data flow, the event system and
offline delivery semantics.
