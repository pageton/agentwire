---
name: agentwire
description: Use when connected to an AgentWire MCP server (tools like register_agent, get_briefing, claim_task, update_progress, ask_agent, record_decision) or when working on a project with multiple AI agents coordinating in parallel. Covers the session-start protocol, delta briefings, progress reporting, file-overlap warnings, messaging etiquette, shared decisions/context, task notes, and structured handoffs on completion.
---

# AgentWire coordination protocol

AgentWire connects multiple AI agents working in parallel on the same
project. It never executes code, never blocks tasks, and never merges
files — it only connects, communicates, and records. **You are the
intelligence; parallel work is the default.** Never wait for another
agent's task to finish. Dependencies are informational: communicate them
in messages, don't gate on them.

## Session start — do these in order

1. `register_agent(agent_id, name, project_id=...)` — your persistent
   identity. Use the same stable `agent_id` every session (e.g. the one
   you were told, otherwise pick `agent-N`). Never create a second id.
2. `get_briefing(project_id, agent_id)` — your task, other active agents
   with progress, recent messages, unacknowledged inbox count, active
   decisions, recently completed tasks with handoffs, file-overlap
   warnings. The briefing ends with `Latest event id: N` — remember it;
   pass it as `since_event_id` next session to get only what changed since.
3. `get_unread(agent_id)` — drain your durable inbox: every event that
   concerns you and was never acknowledged (a WebSocket adapter delivers
   these as a `replay` automatically). Acknowledge with `ack_event` after
   processing.
4. `list_context(project_id)` or `list_context(project_id, prefix="api/")` —
   discover shared context without knowing exact keys.
5. `list_tasks(project_id)` then `claim_task(task_id, agent_id)` — start
   immediately. Claiming never waits, even if related tasks are pending.
6. If the task (or a related completed one) has prior work:
   `get_task(task_id)` — description, handoff, completion summary, notes,
   and the values of any context keys the handoff references.

## While working

- **Report progress at every meaningful step**, not just the end:
  `update_progress(task_id, progress, activity, agent_id)`. Keep
  `activity` human-readable and specific ("Implemented the wakeup
  decision table", not "working").
- **Record durable facts on the task as you learn them**:
  `add_task_note(task_id, note, agent_id)` — interface facts, gotchas,
  blockers. Notes are append-only and survive message history; the next
  claimant reads the task, not 40 messages.
- **Report files you're touching** whenever the set changes:
  `set_files(agent_id, files)` — pass the FULL list each time (it
  replaces the previous one).
- **Check for collisions** before editing a new file:
  `get_file_overlaps(project_id)`. Warnings are visibility only —
  negotiate with the other agent via `ask_agent` if you both need the
  same file.
- **Before asking a question**, check `list_context(project_id, search=...)`
  and `get_decisions(project_id, active_only=true)` — another agent may
  already have answered. Knowledge you can't find doesn't exist; search
  before you ask.
- **After every important decision you make** (API shape, naming,
  behavior that affects others): `record_decision(project_id, title,
  decision, reason, agent_id)`. If a decision replaces an older one,
  pass `supersedes=<decision id>` — briefings then show only the
  decision currently in force.
- **Store reusable facts** with `set_context(project_id, key, value)` and
  a namespaced key (e.g. `api/recoverer.clock`) so others can find them
  via `list_context`.

## Coordinating with other agents

- Question with a thread: `ask_agent(to_agent, question, from_agent)`.
- Answer: `reply_message(message_id, content, from_agent)` — inherits the
  thread, addresses the original sender.
- Broad/typed updates: `send_message(...)` with `type`:
  `message`, `question`, `answer`, `progress`, `instruction`,
  `decision`, `warning`, `interface_change`, `blocked`, `completed`.
  - Omit `to_agents` to broadcast to the whole project.
  - `to_agents: "agent-1,agent-2"` sends to several.
- When you publish a public interface or change one, send an
  `interface_change` broadcast so downstream agents adapt, and add a
  task note recording the interface.

## The inbox — monitoring incoming events

Everything that concerns you lands in your **durable inbox**: messages,
questions, instructions, progress, interface changes, decisions, task
completions and handoffs. Events stay there until you acknowledge them —
unacknowledged events are re-delivered after a reconnect.

- **At session start**, read your inbox: `get_unread(agent_id)` (your
  WebSocket adapter usually delivers a `replay` automatically).
- **WebSocket-connected agents** receive live events automatically; the
  runtime injects them at the next turn. Acknowledge after processing by
  sending `{"type":"ack","event_id":N}` over the socket — or call
  `ack_event(agent_id, event_id)` from MCP.
- **MCP-only agents** (no WebSocket): drain with `get_unread(agent_id)`,
  then block for the next event with `wait_for_events(agent_id,
  timeout_ms=30000)`. Acknowledge with `ack_event(agent_id, event_id)`
  after processing (omit `event_id` to acknowledge everything).
- `get_messages(unread_only=true, agent_id=...)` reads just the message
  part of the inbox; `mark_read` acknowledges everything.
- **Ack means "processed"** — never acknowledge an event you have not
  actually read and acted on. Unacknowledged events are re-delivered on
  purpose.
- Never silently ignore a `question` or `instruction` addressed to you:
  answer it (`reply_message`) or acknowledge explicitly.

## When blocked

- `update_progress(task_id, progress, activity, agent_id,
  status="blocked")` so the team sees you're waiting.
- Broadcast a `blocked` message naming what you need and from whom.
- Add a task note describing exactly what unblocks you.
- Keep working on anything else you own — never idle-wait.

## Finishing — leave a handoff

When completing a task, don't just mark it done. Give the next agent
everything they need in one call:

```
complete_task(task_id, agent_id,
  summary="what was accomplished",          # required for a handoff
  changed_files=["src/td/expiry.rs", ...],
  details=["important implementation details", ...],
  decisions_made=["decisions taken during the work", ...],
  next_steps=["remaining work / what to do next", ...],
  context_keys=["api/clock", ...])          # keys stored via set_context
```

- The handoff is stored on the task and broadcast live
  (`task.handoff` event); offline agents get it in their briefing
  ("Recently completed") or via `get_task(task_id)`.
- Durable decisions should ALSO be recorded via `record_decision` —
  `decisions_made` in the handoff is a convenience summary.
- Handoffs are informational: they never block or trigger other tasks.
- After completing: `set_files(agent_id, files=[])` to clear your list.
- Before starting a task someone else completed, read
  `get_task(task_id)` instead of asking what happened.

## Shared memory

- `set_context(project_id, key, value)` / `get_context(project_id)`
  (optionally with `key`) — shared key/value project memory. Use for
  facts everyone needs (build commands, known gotchas).
- `list_context(project_id, prefix=..., search=..., limit=...)` —
  discover context by key prefix and/or key/value substring. Always
  search here before asking a teammate.
- `record_decision` / `get_decisions` — durable decisions with reasons;
  supersede outdated ones instead of recording contradictions.

## Tool reference

| tool | purpose |
|---|---|
| `register_agent` | create/update persistent identity (session start) |
| `list_agents` | roster with presence, task, files |
| `set_files` / `get_file_overlaps` | file awareness, overlap warnings |
| `list_tasks` / `create_task` | task state |
| `claim_task` | start a task (never blocks) |
| `update_progress` | progress % + activity, pushed to the team |
| `complete_task` | mark done (100%); optionally record a structured handoff |
| `get_task` | task details: handoff, completion summary, notes, context values |
| `add_task_note` / `get_task_notes` | durable per-task knowledge |
| `send_message` | direct (1 or many) or broadcast, 10 message types |
| `ask_agent` | question with thread |
| `reply_message` | answer, inherits thread |
| `get_unread` | your durable inbox: unacknowledged events, oldest first |
| `ack_event` | acknowledge inbox events (through `event_id`, or everything) |
| `wait_for_events` | block until new inbox events or timeout (no WebSocket needed) |
| `get_messages` / `mark_read` | message history; `unread_only` reads the inbox, `mark_read` acks it |
| `get_project_activity` | full live picture of the project |
| `get_briefing` | startup briefing; `since_event_id` for a delta briefing |
| `record_decision` / `get_decisions` | shared decisions; supersession supported |
| `set_context` / `get_context` | shared key/value context |
| `list_context` | discover context by key prefix / key or value substring |

## Etiquette

- Work in parallel. Claimed tasks never gate each other.
- Progress is cheap; silence is expensive. Update at every checkpoint.
- Prefer `reply_message` over `send_message` when answering.
- Record decisions once — teammates read them instead of re-asking.
- Search (`list_context`, `get_decisions`, `get_task`) before you ask.
- Leave a handoff on completion — it is the difference between the next
  agent starting informed or re-reading the whole history.
- If you and another agent overlap on a file, negotiate explicitly with
  a message; AgentWire will not stop either of you.
