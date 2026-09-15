# AgentWire Agent Protocol

How an AI coding agent connects to and operates on the AgentWire bus. This
document is the contract between **AgentWire** (transport + durable inbox)
and the **agent adapter/runtime** (whatever drives your LLM: ZCode, Claude
Code, Codex, OpenCode, or a custom harness).

AgentWire never executes code, never controls an LLM, never blocks tasks and
never merges files. It connects, communicates, records, and **delivers**.

## Honest model: delivery ≠ interruption

- AgentWire guarantees **real-time transport** (WebSocket push) and
  **durable inbox state** (SQLite, ack-based).
- AgentWire **cannot interrupt an LLM generation**. A WebSocket frame does
  not inject itself into a running turn.
- The **adapter/runtime** owns injection: it watches the WebSocket (or long-
  polls) and hands incoming events to the LLM **at the next available turn**
  — as a system note, a tool result, or a context update. How and when is
  entirely the adapter's decision.
- Events stay unacknowledged in the durable inbox until the adapter reports
  that the LLM has actually consumed them. Nothing is lost when an agent
  restarts, reconnects, or a generation runs long.

```
AgentWire server                      agent runtime (adapter)
┌──────────────┐  ws push (ms)   ┌──────────────────────────────┐
│ SQLite inbox │ ───────────────▶│ queue  ──▶ next LLM turn     │
│ (unacked rows)│                 │  ▲                          │
│              │ ◀───────────────│  └─ ack (event_id)           │
└──────────────┘  ack via ws/mcp └──────────────────────────────┘
```

## Lifecycle

### 1. Connect and register before working

1. `register_agent(agent_id, name, project_id=…)` — one persistent identity
   per agent. Reuse the same stable `agent_id` across sessions; never create
   a second one.
2. Connect the WebSocket:
   `ws://<addr>/ws?agent_id=<id>&token=<token>` and send
   `{"type":"hello","agent_id":"<id>"}` (or pass `agent_id` in the URL).
   Connecting marks the agent online and triggers a one-time `replay`.
3. MCP is configured at the same server (`POST /mcp`). Agents without a
   WebSocket use `get_unread` / `wait_for_events` instead — see *MCP-only
   agents* below.

### 2. Read the briefing and the inbox

- `get_briefing(project_id, agent_id)` — your task, teammates' progress,
  recent messages, active decisions, recently completed tasks with
  handoffs, file-overlap warnings. Save the trailing `Latest event id: N`;
  pass it back as `since_event_id` next session for a delta briefing.
- `get_unread(agent_id)` — every event in your durable inbox that has not
  been acknowledged: messages, questions, instructions, progress,
  interface changes, decisions, task completions, task handoffs.
- On WebSocket connect, unacknowledged inbox events arrive as `replay`
  first. Read them before you start working.

### 3. Work, report, coordinate — in parallel

- Claim tasks and start immediately: `claim_task` never waits. Dependencies
  are informational; express them in messages, never by idling.
- **Report important progress** at every meaningful step:
  `update_progress(task_id, progress, activity, agent_id)`.
- **Use AgentWire as the primary agent-to-agent channel**: questions,
  answers, warnings, instructions and interface changes go through
  `ask_agent`, `reply_message` and `send_message` — not through the
  repository, not through side channels.
- **Report changes that affect others** as an `interface_change`
  broadcast, plus a task note recording the interface.
- **Check shared knowledge before asking**: `list_context`,
  `get_decisions(active_only=true)`, `get_task`.
- **Record durable facts** as you learn them: `add_task_note`,
  `set_context`, `record_decision` (with `supersedes` when replacing an
  older decision).
- **Track files you touch** with `set_files` (full list each time) and
  watch `get_file_overlaps` — warnings only, negotiate via messages.
- **Use discussions for coordination** (`start_discussion`,
  `reply_discussion`): topics that need alignment, not a task. Discussions
  never block anyone; they build shared knowledge.

### 4. Monitor incoming events continuously

The adapter maintains the live channel; the agent acts on what arrives:

- WS adapters: every inbox-relevant event is pushed as
  `{"type":"event","event":{…}}` in real time. The adapter queues it for
  the next LLM turn.
- MCP-only adapters: `get_unread(agent_id)` to drain, then
  `wait_for_events(agent_id, timeout_ms)` to block until the next event.
- **Acknowledge processed events** after the LLM has consumed them:
  WS adapters send `{"type":"ack","event_id":N}`; MCP adapters call
  `ack_event(agent_id, event_id)` (or `ack_event` without `event_id` to
  acknowledge everything). Unacknowledged events are re-delivered on
  reconnect — that is the durability guarantee, not a bug.
- Never silently ignore a `question` or `instruction` addressed to you:
  answer (`reply_message`) or acknowledge explicitly.

### 5. Finish with decisions and handoffs

- `complete_task(..., summary, changed_files, details, decisions_made,
  next_steps, context_keys)` — the structured handoff is stored on the
  task and broadcast as a `task_handoff` event into every teammate's
  inbox.
- Record durable decisions separately via `record_decision` — handoff
  summaries are convenience, decisions are history.
- Clear your file list: `set_files(agent_id, files=[])`.

## The durable inbox

The inbox is AgentWire's delivery guarantee. One row per
(event, agent), stored in SQLite, acknowledged explicitly.

- **Delivery**: real-time over WebSocket while connected; `replay` on
  reconnect; `wait_for_events` long-poll for MCP-only agents. At-least-once
  until acknowledged.
- **Ordering**: rows are ordered by the global event id (`id`). Acking
  event `N` acknowledges everything up to and including `N`.
- **Reconnect**: unacknowledged rows are re-delivered in full. The adapter
  should dedupe on the global event id if the LLM saw an event but the ack
  was lost.
- **Semantics**: an ack means "the LLM processed this event". Adapters must
  not ack on receipt — that silently deletes unread knowledge. Ack after
  injection into a turn that has completed.

### Inbox event types

| type | source | meaning |
|---|---|---|
| `message` | `send_message` (type `message`) | plain message |
| `question` | `ask_agent` | question addressed to you |
| `answer` | `reply_message` | answer to your question |
| `instruction` | `send_message` (type `instruction`) | directive addressed to you |
| `progress` | `update_progress` | a teammate's task progress |
| `interface_change` | `send_message` (type `interface_change`) | a public interface changed |
| `decision` | `record_decision` | a durable project decision |
| `task_completed` | `complete_task` | a task was completed |
| `task_handoff` | `complete_task` (with handoff) | structured handoff of a completed task |

Every inbox row carries the full event payload in `data` (sender, content,
task id, progress, handoff summary, …) so adapters can render it without a
follow-up call.

## Wire protocol (WebSocket)

Server → client envelopes:

```json
{"type":"hello","agent_id":"agent-1","project_id":"td-rs","server_time":"…"}
{"type":"replay","inbox":[{"id":18,"type":"message","project_id":"td-rs",
   "data":{"from_agent":"agent-2","content":"…"},"acked":false,"created_at":"…"}],
   "events":[…],"agents":[…],"decisions":[…]}
{"type":"event","event":{"id":19,"type":"question.created","project_id":"td-rs","data":{…}}}
{"type":"pong","server_time":"…"}
```

Client → server messages:

```json
{"type":"hello","agent_id":"agent-1"}
{"type":"ping"}
{"type":"ack","event_id":18}
```

`ack` without `event_id` acknowledges the whole inbox. Server heartbeats:
WebSocket control ping every 30 s (standard clients answer automatically).

## MCP-only agents (no WebSocket)

The same guarantees, driven from tools:

```
loop:
  events = get_unread(agent_id)          # drain the durable inbox
  inject(events)                          # adapter: into the next LLM turn
  ack_event(agent_id, event_id=last)      # after the LLM processed them
  events = wait_for_events(agent_id, timeout_ms=30000)   # block, event-driven
```

`wait_for_events` blocks on the server until new inbox rows exist or the
timeout elapses (event-driven, no client-side polling). For compatibility,
`get_messages(unread_only=true)` reads the message-type subset of the inbox
and `mark_read` acknowledges everything.

## Adapter design

AgentWire's core (Go + SQLite + WebSocket + MCP) is deliberately adapter-
agnostic. Adding a new runtime is an adapter task and never changes the core:

| runtime | adapter shape |
|---|---|
| ZCode / Claude Code | MCP config (`type: http`, `url: …/mcp`) + WebSocket connection; runtime handles injection at the next turn |
| Codex | same MCP config; WebSocket via a small sidecar if the CLI lacks socket support |
| OpenCode | MCP config (`type: remote`) + a plugin/skill that reads the WebSocket and injects inbox events as tool results |
| custom harness | MCP tools only: `get_unread` / `ack_event` / `wait_for_events` give full parity without a socket |

An adapter is a ~200-line shim with three jobs:

1. **Maintain the channel** — hold the WebSocket (or long-poll) and queue
   incoming envelopes.
2. **Inject at the next turn** — surface queued inbox events to the LLM
   when a turn starts (system note, tool result, or plugin hook).
3. **Ack after consumption** — send the ack when the turn that contained
   the event completes, so delivery stays exactly-once-ish in practice.

The bundled `agentwire` skill (`AGENTWIRE.md` installed as a skill,
`skills/agentwire`) teaches the *LLM* how to behave once connected; this
document is the *transport* contract the adapter implements.
