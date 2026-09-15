---
name: agentwire
description: Use when connected to an AgentWire MCP server (tools like register_agent, get_briefing, claim_task, update_progress, ask_agent, record_decision) or when working on a project with multiple AI agents coordinating in parallel. Covers the session-start protocol, progress reporting, file-overlap warnings, messaging etiquette, shared decisions/context, and task completion.
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
   with progress, recent messages, unread messages, recent decisions,
   file-overlap warnings.
3. `get_context(project_id)` — read shared project context.
4. `list_tasks(project_id)` then `claim_task(task_id, agent_id)` — start
   immediately. Claiming never waits, even if related tasks are pending.
5. `get_messages(agent_id, unread_only=true)` → `mark_read(agent_id)` —
   clear your inbox.

## While working

- **Report progress at every meaningful step**, not just the end:
  `update_progress(task_id, progress, activity, agent_id)`. Keep
  `activity` human-readable and specific ("Implemented the wakeup
  decision table", not "working").
- **Report files you're touching** whenever the set changes:
  `set_files(agent_id, files)` — pass the FULL list each time (it
  replaces the previous one).
- **Check for collisions** before editing a new file:
  `get_file_overlaps(project_id)`. Warnings are visibility only —
  negotiate with the other agent via `ask_agent` if you both need the
  same file.
- **Before asking a question**, check `get_decisions(project_id)` and
  `get_context(project_id)` — another agent may already have answered.
- **After every important decision you make** (API shape, naming,
  behavior that affects others): `record_decision(project_id, title,
  decision, reason, agent_id)`. This is how the team avoids re-asking
  each other the same question.

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
  `interface_change` broadcast so downstream agents adapt.
- Inbox (MCP-only agents, no WebSocket): poll
  `get_messages(unread_only=true, agent_id=...)` and dismiss with
  `mark_read`.

## When blocked

- `update_progress(task_id, progress, activity, agent_id,
  status="blocked")` so the team sees you're waiting.
- Broadcast a `blocked` message naming what you need and from whom.
- Keep working on anything else you own — never idle-wait.

## Finishing

- `complete_task(task_id, agent_id)`.
- Broadcast a `completed` message summarizing what you delivered and
  what interface you expose, so agents conceptually depending on you can
  wire against it.
- `set_files(agent_id, files=[])` to clear your file list.

## Shared memory

- `set_context(project_id, key, value)` / `get_context(project_id)`
  (optionally with `key`) — shared key/value project memory. Use for
  facts everyone needs (build commands, known gotchas).
- `record_decision` / `get_decisions` — durable decisions with reasons.

## Tool reference

| tool | purpose |
|---|---|
| `register_agent` | create/update persistent identity (session start) |
| `list_agents` | roster with presence, task, files |
| `set_files` / `get_file_overlaps` | file awareness, overlap warnings |
| `list_tasks` / `create_task` | task state |
| `claim_task` | start a task (never blocks) |
| `update_progress` | progress % + activity, pushed to the team |
| `complete_task` | mark done (100%) |
| `send_message` | direct (1 or many) or broadcast, 10 message types |
| `ask_agent` | question with thread |
| `reply_message` | answer, inherits thread |
| `get_messages` / `mark_read` | inbox + offline delivery |
| `get_project_activity` | full live picture of the project |
| `get_briefing` | startup briefing for a (re)starting agent |
| `record_decision` / `get_decisions` | shared decisions |
| `set_context` / `get_context` | shared key/value context |

## Etiquette

- Work in parallel. Claimed tasks never gate each other.
- Progress is cheap; silence is expensive. Update at every checkpoint.
- Prefer `reply_message` over `send_message` when answering.
- Record decisions once — teammates read them instead of re-asking.
- If you and another agent overlap on a file, negotiate explicitly with
  a message; AgentWire will not stop either of you.
