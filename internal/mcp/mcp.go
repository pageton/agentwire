// Package mcp exposes AgentWire as an MCP server. All tools are plain
// coordination primitives: they read/write shared state and emit events.
// AgentWire never executes code or controls an LLM.
package mcp

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"agentwire/internal/agent"
	"agentwire/internal/db"
	"agentwire/internal/event"
	"agentwire/internal/inbox"
	"agentwire/internal/message"
	"agentwire/internal/task"
)

const serverVersion = "0.2.0"

// Deps bundles the stores and hub the tools operate on.
type Deps struct {
	DB     *db.Store
	Hub    *event.Hub
	Agents *agent.Store
	Tasks  *task.Store
	Msgs   *message.Store
	Inbox  *inbox.Store
}

// New builds the MCP server with all AgentWire tools registered.
func New(deps Deps) *server.MCPServer {
	s := server.NewMCPServer("agentwire", serverVersion,
		server.WithToolCapabilities(false),
	)

	// ---- agents ----
	s.AddTool(mcp.NewTool("register_agent",
		mcp.WithDescription("Register your agent with AgentWire (call once at session start). "+
			"Creates or updates a persistent agent identity tied to a project. Your agent_id is your "+
			"callsign for all other tools; pick a stable id like 'agent-1'."),
		mcp.WithString("agent_id", mcp.Description("Stable unique agent id, e.g. 'agent-1'"), mcp.Required()),
		mcp.WithString("name", mcp.Description("Human-readable agent name"), mcp.Required()),
		mcp.WithString("type", mcp.Description("Agent type, default 'coding'")),
		mcp.WithString("project_id", mcp.Description("Project this agent works on")),
	), withDeps(deps, handleRegisterAgent))

	s.AddTool(mcp.NewTool("list_agents",
		mcp.WithDescription("List agents. Shows identity, presence status, current task and files "+
			"being modified. Filter by project_id to see who is working alongside you."),
		mcp.WithString("project_id", mcp.Description("Filter by project (optional)")),
	), withDeps(deps, handleListAgents))

	s.AddTool(mcp.NewTool("set_files",
		mcp.WithDescription("Report the files you are currently modifying. Other agents see this to "+
			"detect overlap. Pass the full list each time; it replaces the previous list. "+
			"Use an empty array to clear."),
		mcp.WithString("agent_id", mcp.Description("Your agent id"), mcp.Required()),
		mcp.WithArray("files", mcp.Description("File paths, e.g. [\"src/td/config_manager.rs\"]"),
			mcp.WithStringItems(mcp.Description("A file path"))),
	), withDeps(deps, handleSetFiles))

	s.AddTool(mcp.NewTool("get_file_overlaps",
		mcp.WithDescription("Show files modified by more than one agent in a project. "+
			"Warnings only — AgentWire never merges or locks files."),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
	), withDeps(deps, handleGetFileOverlaps))

	// ---- tasks ----
	s.AddTool(mcp.NewTool("list_tasks",
		mcp.WithDescription("List tasks, optionally filtered by project, status "+
			"(pending|working|blocked|completed) or assigned agent."),
		mcp.WithString("project_id", mcp.Description("Filter by project (optional)")),
		mcp.WithString("status", mcp.Description("Filter by status (optional)")),
		mcp.WithString("assigned_to", mcp.Description("Filter by agent id (optional)")),
	), withDeps(deps, handleListTasks))

	s.AddTool(mcp.NewTool("create_task",
		mcp.WithDescription("Create a task in a project. Tasks never block each other: any agent may "+
			"claim and start a task immediately, even if related tasks are unfinished. "+
			"Express informational dependencies in the description."),
		mcp.WithString("task_id", mcp.Description("Unique task id, e.g. 'M.3.7.2'"), mcp.Required()),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
		mcp.WithString("title", mcp.Description("Short task title"), mcp.Required()),
		mcp.WithString("description", mcp.Description("Details, coordination notes, informational dependencies")),
		mcp.WithString("assigned_to", mcp.Description("Pre-assign to an agent id (optional)")),
		mcp.WithString("created_by", mcp.Description("Agent id that creates the task (optional)")),
	), withDeps(deps, handleCreateTask))

	s.AddTool(mcp.NewTool("claim_task",
		mcp.WithDescription("Claim a task and start working on it. Sets status to 'working' and assigns "+
			"the task to you. Never waits for other tasks — parallel work is the default."),
		mcp.WithString("task_id", mcp.Description("Task id to claim"), mcp.Required()),
		mcp.WithString("agent_id", mcp.Description("Your agent id"), mcp.Required()),
	), withDeps(deps, handleClaimTask))

	s.AddTool(mcp.NewTool("update_progress",
		mcp.WithDescription("Publish progress on your task (0-100) with a short human-readable activity "+
			"description. All agents in the project receive the update immediately. Optionally set "+
			"status to 'blocked' when you are waiting on another agent."),
		mcp.WithString("task_id", mcp.Description("Your task id"), mcp.Required()),
		mcp.WithNumber("progress", mcp.Description("0-100 percentage"), mcp.Required()),
		mcp.WithString("activity", mcp.Description("What you are doing right now, e.g. 'Implementing the wakeup decision table'")),
		mcp.WithString("agent_id", mcp.Description("Your agent id"), mcp.Required()),
		mcp.WithString("status", mcp.Description("Optional task status override (working|blocked)")),
	), withDeps(deps, handleUpdateProgress))

	s.AddTool(mcp.NewTool("complete_task",
		mcp.WithDescription("Mark your task completed (progress becomes 100). All agents in the project "+
			"are notified. Optionally leave a structured handoff so the next agent immediately knows what "+
			"was done and what to do next: summary (stored as the task's completion summary), changed_files, "+
			"details (important implementation details), decisions_made, next_steps, and context_keys "+
			"(keys previously stored with set_context). Handoffs are informational — they never block or "+
			"trigger other tasks."),
		mcp.WithString("task_id", mcp.Description("Task id to complete"), mcp.Required()),
		mcp.WithString("agent_id", mcp.Description("Your agent id"), mcp.Required()),
		mcp.WithString("summary", mcp.Description("What was accomplished (required when passing handoff fields)")),
		mcp.WithArray("changed_files", mcp.Description("Files you changed"),
			mcp.WithStringItems(mcp.Description("A file path"))),
		mcp.WithArray("details", mcp.Description("Important implementation details"),
			mcp.WithStringItems(mcp.Description("One detail"))),
		mcp.WithArray("decisions_made", mcp.Description("Decisions taken during the work"),
			mcp.WithStringItems(mcp.Description("One decision (record durable ones via record_decision too)"))),
		mcp.WithArray("next_steps", mcp.Description("Remaining work / what to do next"),
			mcp.WithStringItems(mcp.Description("One next step"))),
		mcp.WithArray("context_keys", mcp.Description("Relevant shared context keys (see list_context)"),
			mcp.WithStringItems(mcp.Description("A context key"))),
	), withDeps(deps, handleCompleteTask))

	s.AddTool(mcp.NewTool("get_task",
		mcp.WithDescription("Get full details of a task: description, progress, completion summary, the "+
			"structured handoff left by the finishing agent (what was done, what changed, next steps), "+
			"task notes, and the values of any context keys referenced by the handoff. Read this after "+
			"claiming a task or before building on a completed one."),
		mcp.WithString("task_id", mcp.Description("Task id"), mcp.Required()),
	), withDeps(deps, handleGetTask))

	s.AddTool(mcp.NewTool("add_task_note",
		mcp.WithDescription("Append a durable note to a task: interface facts, blockers, handoff context, "+
			"gotchas. Notes are append-only and visible to every agent that views the task — the next "+
			"claimant reads the task instead of the whole message history. The task must exist."),
		mcp.WithString("task_id", mcp.Description("Task id to annotate"), mcp.Required()),
		mcp.WithString("note", mcp.Description("The note, e.g. 'RecovererEnv exposes unix_time(), my_phone_number(), expect_blocking()'"), mcp.Required()),
		mcp.WithString("agent_id", mcp.Description("Your agent id (optional)")),
	), withDeps(deps, handleAddTaskNote))

	s.AddTool(mcp.NewTool("get_task_notes",
		mcp.WithDescription("Read the notes appended to a task, oldest first. Call this after claiming a "+
			"task to pick up everything previous workers learned."),
		mcp.WithString("task_id", mcp.Description("Task id"), mcp.Required()),
	), withDeps(deps, handleGetTaskNotes))

	// ---- messaging ----
	s.AddTool(mcp.NewTool("send_message",
		mcp.WithDescription("Send a message to another agent, several agents (comma-separated list in "+
			"'to_agents'), or to everyone in the project (leave 'to_agents' empty for a broadcast). "+
			"The recipients are notified immediately over WebSocket; offline agents receive the message "+
			"when they reconnect. Use 'type' to qualify the message: message, question, answer, "+
			"instruction, warning, interface_change, decision, progress, blocked, completed."),
		mcp.WithString("project_id", mcp.Description("Project id (inferred from your agent if omitted)")),
		mcp.WithString("content", mcp.Description("Message body"), mcp.Required()),
		mcp.WithString("from_agent", mcp.Description("Your agent id"), mcp.Required()),
		mcp.WithString("to_agents", mcp.Description("Recipient agent id(s), comma-separated. Omit to broadcast to the whole project")),
		mcp.WithString("type", mcp.Description("Message type, default 'message'")),
		mcp.WithNumber("reply_to", mcp.Description("Message id this replies to (optional, use reply_message instead when answering)")),
	), withDeps(deps, handleSendMessage))

	s.AddTool(mcp.NewTool("ask_agent",
		mcp.WithDescription("Ask another agent a question. The target agent receives it immediately as a "+
			"'question' event and can answer with reply_message. A thread is created so the conversation "+
			"is easy to follow."),
		mcp.WithString("to_agent", mcp.Description("Agent id to ask"), mcp.Required()),
		mcp.WithString("question", mcp.Description("Your question, e.g. 'What API will X expose?'"), mcp.Required()),
		mcp.WithString("from_agent", mcp.Description("Your agent id"), mcp.Required()),
		mcp.WithString("project_id", mcp.Description("Project id (inferred from your agent if omitted)")),
	), withDeps(deps, handleAskAgent))

	s.AddTool(mcp.NewTool("reply_message",
		mcp.WithDescription("Answer a message or question (referenced by its message id). Inherits the "+
			"original thread so conversations stay grouped. The original sender is notified immediately."),
		mcp.WithNumber("message_id", mcp.Description("Id of the message you are answering"), mcp.Required()),
		mcp.WithString("content", mcp.Description("Your answer"), mcp.Required()),
		mcp.WithString("from_agent", mcp.Description("Your agent id"), mcp.Required()),
		mcp.WithString("type", mcp.Description("Message type, default 'answer'")),
	), withDeps(deps, handleReplyMessage))

	s.AddTool(mcp.NewTool("get_messages",
		mcp.WithDescription("Read messages. With unread_only=true returns only messages addressed to you "+
			"(or broadcast) that are still unacknowledged in your inbox — call mark_read or ack_event "+
			"afterwards to acknowledge them. With unread_only=false returns recent project history."),
		mcp.WithString("project_id", mcp.Description("Project id (optional, all projects if omitted)")),
		mcp.WithString("agent_id", mcp.Description("Agent id whose inbox to read (optional)")),
		mcp.WithBoolean("unread_only", mcp.Description("Only messages not yet delivered to the agent (default false)")),
		mcp.WithNumber("limit", mcp.Description("Max messages (default 50)")),
	), withDeps(deps, handleGetMessages))

	s.AddTool(mcp.NewTool("mark_read",
		mcp.WithDescription("Acknowledge your whole inbox: mark every unacknowledged inbox event as seen "+
			"(equivalent to ack_event without an event_id). Useful after reading unread messages via "+
			"get_messages."),
		mcp.WithString("agent_id", mcp.Description("Your agent id"), mcp.Required()),
	), withDeps(deps, handleMarkRead))

	// ---- inbox (real-time agent protocol) ----
	s.AddTool(mcp.NewTool("get_unread",
		mcp.WithDescription("Read your inbox: every project event that concerns you and has not been "+
			"acknowledged yet — messages, questions, instructions, progress, interface changes, "+
			"decisions, task completions and task handoffs. Oldest first. Acknowledge with ack_event "+
			"after processing; unacknowledged events are re-delivered after a reconnect."),
		mcp.WithString("agent_id", mcp.Description("Your agent id"), mcp.Required()),
		mcp.WithNumber("limit", mcp.Description("Max events (default 50)")),
	), withDeps(deps, handleGetUnread))

	s.AddTool(mcp.NewTool("ack_event",
		mcp.WithDescription("Acknowledge inbox events: mark the event 'event_id' and everything older as "+
			"processed, or every event when event_id is omitted. Acknowledged events are never "+
			"re-delivered. WebSocket-connected agents can instead send {\"type\":\"ack\",\"event_id\":N}."),
		mcp.WithString("agent_id", mcp.Description("Your agent id"), mcp.Required()),
		mcp.WithNumber("event_id", mcp.Description("Acknowledge this event and all older unacknowledged events (optional)")),
	), withDeps(deps, handleAckEvent))

	s.AddTool(mcp.NewTool("wait_for_events",
		mcp.WithDescription("Block until new inbox events for your agent arrive or the timeout elapses. "+
			"Returns unacknowledged inbox events immediately when any are pending. Event-driven — for "+
			"agents without a WebSocket connection, the way to wait for the next event instead of "+
			"repeatedly calling get_unread."),
		mcp.WithString("agent_id", mcp.Description("Your agent id"), mcp.Required()),
		mcp.WithNumber("timeout_ms", mcp.Description("Max wait in milliseconds (default 30000, max 60000)")),
	), withDepsCtx(deps, handleWaitForEvents))

	// ---- shared state ----
	s.AddTool(mcp.NewTool("get_project_activity",
		mcp.WithDescription("Get a complete live picture of a project: all agents with presence and files, "+
			"all tasks with progress and activity, file-overlap warnings, recent events and recent decisions."),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
	), withDeps(deps, handleProjectActivity))

	s.AddTool(mcp.NewTool("get_briefing",
		mcp.WithDescription("Get a compact startup briefing for your agent: your task, other active agents "+
			"with their progress, recent messages, unread messages and important decisions. Call this when "+
			"you (re)start a session to immediately understand what the team is doing. "+
			"Pass since_event_id (from your previous briefing) to get only what changed since then."),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
		mcp.WithString("agent_id", mcp.Description("Your agent id"), mcp.Required()),
		mcp.WithNumber("since_event_id", mcp.Description("Only changes after this event id (delta briefing); omit for a full briefing")),
	), withDeps(deps, handleBriefing))

	s.AddTool(mcp.NewTool("record_decision",
		mcp.WithDescription("Record an important implementation decision for the project so other agents "+
			"don't ask the same question twice. Include the reason — it helps teammates adapt. "+
			"When a decision replaces an older one, reference it via supersedes so briefings show only "+
			"the current decision trail."),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
		mcp.WithString("title", mcp.Description("Short decision title, e.g. 'RecovererEnv clock injection'"), mcp.Required()),
		mcp.WithString("decision", mcp.Description("What was decided"), mcp.Required()),
		mcp.WithString("reason", mcp.Description("Why — e.g. 'Makes decision-table tests deterministic'")),
		mcp.WithString("agent_id", mcp.Description("Your agent id (optional)")),
		mcp.WithNumber("supersedes", mcp.Description("Decision id this one replaces (optional)")),
	), withDeps(deps, handleRecordDecision))

	s.AddTool(mcp.NewTool("get_decisions",
		mcp.WithDescription("Retrieve recorded implementation decisions for a project. Decisions replaced "+
			"by a newer one are marked superseded; pass active_only=true to hide them."),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
		mcp.WithNumber("limit", mcp.Description("Max decisions (default 20)")),
		mcp.WithBoolean("active_only", mcp.Description("Hide superseded decisions (default false)")),
	), withDeps(deps, handleGetDecisions))

	s.AddTool(mcp.NewTool("set_context",
		mcp.WithDescription("Store a key/value pair in the shared project context (SQLite, no embeddings). "+
			"Use it for important project information other agents should know. Overwrites the previous "+
			"value for the same key."),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
		mcp.WithString("key", mcp.Description("Context key"), mcp.Required()),
		mcp.WithString("value", mcp.Description("Context value"), mcp.Required()),
	), withDeps(deps, handleSetContext))

	s.AddTool(mcp.NewTool("get_context",
		mcp.WithDescription("Read shared project context. With a key, returns that single entry; without, "+
			"returns all entries for the project."),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
		mcp.WithString("key", mcp.Description("Specific key (optional)")),
	), withDeps(deps, handleGetContext))

	s.AddTool(mcp.NewTool("list_context",
		mcp.WithDescription("Discover shared project context without knowing exact keys: filter by key "+
			"prefix (e.g. 'api/') and/or a substring of key or value. Returns key, value and last-updated. "+
			"Use this before asking a teammate something that may already be recorded."),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
		mcp.WithString("prefix", mcp.Description("Key prefix filter, e.g. 'api/' (optional)")),
		mcp.WithString("search", mcp.Description("Substring to match in key or value (optional)")),
		mcp.WithNumber("limit", mcp.Description("Max entries (default 50)")),
	), withDeps(deps, handleListContext))

	return s
}

type toolFn func(d *Deps, req mcp.CallToolRequest) string

func withDeps(d Deps, h toolFn) server.ToolHandlerFunc {
	return func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		out := h(&d, req)
		if strings.HasPrefix(out, "error:") {
			return mcp.NewToolResultError(strings.TrimPrefix(out, "error: ")), nil
		}
		return mcp.NewToolResultText(out), nil
	}
}

// withDepsCtx is withDeps for handlers that need the request context
// (wait_for_events blocks until ctx, timeout or new inbox events).
type toolFnCtx func(ctx context.Context, d *Deps, req mcp.CallToolRequest) string

func withDepsCtx(d Deps, h toolFnCtx) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		out := h(ctx, &d, req)
		if strings.HasPrefix(out, "error:") {
			return mcp.NewToolResultError(strings.TrimPrefix(out, "error: ")), nil
		}
		return mcp.NewToolResultText(out), nil
	}
}

// ---- argument helpers ----

func str(args map[string]any, key string) string {
	v, _ := args[key].(string)
	return strings.TrimSpace(v)
}

func num(args map[string]any, key string) int {
	v, _ := args[key].(float64)
	return int(v)
}

func boolean(args map[string]any, key string) bool {
	v, _ := args[key].(bool)
	return v
}

func strList(args map[string]any, key string) []string {
	raw, ok := args[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// agentFrom resolves the acting agent id, falling back to the X-Agent-Id
// HTTP header (set by clients that support headers on MCP requests).
func agentFrom(args map[string]any, req mcp.CallToolRequest, key string) string {
	if id := str(args, key); id != "" {
		return id
	}
	return req.Header.Get("X-Agent-Id")
}

// projectOf resolves the project: explicit argument, then the agent's project.
func (d *Deps) projectOf(explicit, agentID string) string {
	if explicit != "" {
		return explicit
	}
	if agentID != "" {
		if a, err := d.Agents.Get(agentID); err == nil && a != nil {
			return a.ProjectID
		}
	}
	return ""
}

// ---- agents ----

func handleRegisterAgent(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	id := str(args, "agent_id")
	name := str(args, "name")
	if id == "" || name == "" {
		return "error: agent_id and name are required"
	}
	typ := str(args, "type")
	proj := str(args, "project_id")
	if proj != "" {
		if err := d.DB.EnsureProject(proj); err != nil {
			return "error: " + err.Error()
		}
	}
	existing, _ := d.Agents.Get(id)
	if existing == nil {
		existing = &agent.Agent{AgentID: id, Status: agent.StatusOffline}
	}
	if err := d.Agents.Register(agent.Agent{
		AgentID:   id,
		Name:      name,
		Type:      typ,
		Status:    existing.Status,
		ProjectID: proj,
	}); err != nil {
		return "error: " + err.Error()
	}
	return fmt.Sprintf("registered agent %s (%s) in project %s", id, name, orDash(proj))
}

func handleListAgents(d *Deps, req mcp.CallToolRequest) string {
	agents, err := d.Agents.List(str(req.GetArguments(), "project_id"))
	if err != nil {
		return "error: " + err.Error()
	}
	if len(agents) == 0 {
		return "no agents registered"
	}
	var b strings.Builder
	b.WriteString("id | name | status | project | task | files\n")
	for _, a := range agents {
		fmt.Fprintf(&b, "%s | %s | %s | %s | %s | %s\n",
			a.AgentID, orDash(a.Name), a.Status, orDash(a.ProjectID),
			orDash(a.CurrentTask), orDash(strings.Join(a.Files, ", ")))
	}
	return b.String()
}

func handleSetFiles(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	id := agentFrom(args, req, "agent_id")
	if id == "" {
		return "error: agent_id is required"
	}
	files := strList(args, "files")
	if err := d.Agents.SetFiles(id, files); err != nil {
		return "error: " + err.Error()
	}
	proj := d.projectOf("", id)
	d.Hub.Publish(event.AgentFilesChanged, proj, map[string]any{"agent": id, "files": files})

	overlaps, _ := d.Agents.Overlaps(proj)
	for _, o := range overlaps {
		d.Hub.Publish(event.WarningFileOverlap, proj, map[string]any{"file": o.File, "agents": o.Agents})
	}
	if len(overlaps) > 0 {
		return fmt.Sprintf("files recorded. WARNING: %d file(s) are modified by multiple agents: %s",
			len(overlaps), formatOverlaps(overlaps))
	}
	return fmt.Sprintf("files recorded for %s: %s", id, orDash(strings.Join(files, ", ")))
}

func handleGetFileOverlaps(d *Deps, req mcp.CallToolRequest) string {
	proj := str(req.GetArguments(), "project_id")
	if proj == "" {
		return "error: project_id is required"
	}
	overlaps, err := d.Agents.Overlaps(proj)
	if err != nil {
		return "error: " + err.Error()
	}
	if len(overlaps) == 0 {
		return "no file overlaps in project " + proj
	}
	return formatOverlaps(overlaps)
}

func formatOverlaps(overlaps []agent.FileOverlap) string {
	var b strings.Builder
	for _, o := range overlaps {
		fmt.Fprintf(&b, "⚠ %s — modified by: %s\n", o.File, strings.Join(o.Agents, ", "))
	}
	return b.String()
}

// ---- tasks ----

func handleListTasks(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	tasks, err := d.Tasks.List(str(args, "project_id"), str(args, "status"), str(args, "assigned_to"))
	if err != nil {
		return "error: " + err.Error()
	}
	if len(tasks) == 0 {
		return "no tasks match"
	}
	var b strings.Builder
	b.WriteString("task_id | project | status | assigned | progress | activity | title\n")
	for _, t := range tasks {
		fmt.Fprintf(&b, "%s | %s | %s | %s | %d%% | %s | %s\n",
			t.TaskID, t.ProjectID, t.Status, orDash(t.AssignedTo), t.Progress,
			orDash(t.CurrentActivity), orDash(t.Title))
	}
	return b.String()
}

func handleCreateTask(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	taskID := str(args, "task_id")
	proj := str(args, "project_id")
	if taskID == "" || proj == "" {
		return "error: task_id and project_id are required"
	}
	if err := d.DB.EnsureProject(proj); err != nil {
		return "error: " + err.Error()
	}
	status := task.StatusPending
	if str(args, "assigned_to") != "" {
		status = task.StatusWorking
	}
	t := task.Task{
		TaskID:      taskID,
		ProjectID:   proj,
		Title:       str(args, "title"),
		Description: str(args, "description"),
		Status:      status,
		AssignedTo:  str(args, "assigned_to"),
		CreatedBy:   str(args, "created_by"),
	}
	if err := d.Tasks.Create(t); err != nil {
		return "error: " + err.Error()
	}
	d.Hub.Publish(event.TaskCreated, proj, taskData(&t))
	return fmt.Sprintf("task %s created in %s (%s)", taskID, proj, t.Status)
}

func handleClaimTask(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	taskID := str(args, "task_id")
	id := agentFrom(args, req, "agent_id")
	if taskID == "" || id == "" {
		return "error: task_id and agent_id are required"
	}
	t, err := d.Tasks.Claim(taskID, id)
	if err != nil {
		return "error: " + err.Error()
	}
	if err := d.Agents.SetCurrentTask(id, taskID); err == nil {
		d.Hub.Publish(event.AgentStatusChange, t.ProjectID, map[string]any{
			"agent": id, "status": agent.StatusOnline, "current_task": taskID,
		})
	}
	d.Hub.Publish(event.TaskClaimed, t.ProjectID, taskData(t))
	return fmt.Sprintf("task %s claimed by %s (working). You may start immediately — tasks never block each other.", taskID, id)
}

func handleUpdateProgress(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	taskID := str(args, "task_id")
	id := agentFrom(args, req, "agent_id")
	if taskID == "" || id == "" {
		return "error: task_id and agent_id are required"
	}
	t, err := d.Tasks.UpdateProgress(taskID, num(args, "progress"), str(args, "activity"), str(args, "status"))
	if err != nil {
		return "error: " + err.Error()
	}
	if t == nil {
		return "error: task " + taskID + " not found"
	}
	typ := event.TaskProgress
	if t.Status == task.StatusBlocked {
		typ = event.TaskBlocked
	} else if t.Status == task.StatusCompleted {
		typ = event.TaskCompleted
	}
	d.Hub.Publish(typ, t.ProjectID, map[string]any{
		"task_id": t.TaskID, "project_id": t.ProjectID, "title": t.Title,
		"status": t.Status, "assigned_to": t.AssignedTo, "progress": t.Progress,
		"current_activity": t.CurrentActivity, "agent": id,
	})
	return fmt.Sprintf("task %s: %d%% — %s", t.TaskID, t.Progress, orDash(t.CurrentActivity))
}

func handleCompleteTask(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	taskID := str(args, "task_id")
	id := agentFrom(args, req, "agent_id")
	if taskID == "" || id == "" {
		return "error: task_id and agent_id are required"
	}

	// Handoff mode kicks in when any handoff field is provided.
	h := &task.Handoff{
		Summary:       str(args, "summary"),
		ChangedFiles:  strList(args, "changed_files"),
		Details:       strList(args, "details"),
		DecisionsMade: strList(args, "decisions_made"),
		NextSteps:     strList(args, "next_steps"),
		ContextKeys:   strList(args, "context_keys"),
	}
	hasHandoff := h.Summary != "" || len(h.ChangedFiles) > 0 || len(h.Details) > 0 ||
		len(h.DecisionsMade) > 0 || len(h.NextSteps) > 0 || len(h.ContextKeys) > 0
	if hasHandoff && h.Summary == "" {
		return "error: summary is required when leaving a handoff"
	}

	var t *task.Task
	var err error
	if hasHandoff {
		t, err = d.Tasks.CompleteWithHandoff(taskID, h.Summary, h)
	} else {
		t, err = d.Tasks.Complete(taskID)
	}
	if err != nil {
		return "error: " + err.Error()
	}
	d.Hub.Publish(event.TaskCompleted, t.ProjectID, map[string]any{
		"task_id": t.TaskID, "project_id": t.ProjectID, "title": t.Title,
		"status": t.Status, "assigned_to": t.AssignedTo, "progress": t.Progress,
		"current_activity": t.CurrentActivity, "agent": id,
		"summary": t.CompletionSummary,
	})
	if !hasHandoff {
		return fmt.Sprintf("task %s completed by %s. Other agents in %s have been notified.", taskID, id, t.ProjectID)
	}
	d.Hub.Publish(event.TaskHandoff, t.ProjectID, map[string]any{
		"task": t.TaskID, "agent": id, "summary": h.Summary,
		"handoff": h,
	})
	return fmt.Sprintf("task %s completed by %s with handoff. Other agents in %s have been notified; "+
		"the handoff is available via get_task(task_id=%q).", taskID, id, t.ProjectID, taskID)
}

func handleGetTask(d *Deps, req mcp.CallToolRequest) string {
	taskID := str(req.GetArguments(), "task_id")
	if taskID == "" {
		return "error: task_id is required"
	}
	t, err := d.Tasks.Get(taskID)
	if err != nil {
		return "error: " + err.Error()
	}
	if t == nil {
		return "task " + taskID + " not found"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s [%s] — %s\n", t.TaskID, t.Status, t.Title)
	fmt.Fprintf(&b, "project: %s | assigned: %s | progress: %d%%%s\n",
		t.ProjectID, orDash(t.AssignedTo), t.Progress, suffix(t.CurrentActivity))
	if t.Description != "" {
		fmt.Fprintf(&b, "description: %s\n", t.Description)
	}
	if t.CompletionSummary != "" {
		fmt.Fprintf(&b, "\nCompletion summary: %s\n", t.CompletionSummary)
	}
	if t.Handoff != nil {
		b.WriteString("\nHandoff:\n")
		writeHandoff(&b, t.Handoff)
		if len(t.Handoff.ContextKeys) > 0 {
			b.WriteString("\nContext values:\n")
			for _, k := range t.Handoff.ContextKeys {
				entries, err := d.DB.GetContext(t.ProjectID, k)
				if err != nil || len(entries) == 0 {
					fmt.Fprintf(&b, "  %s: (no context entry)\n", k)
					continue
				}
				fmt.Fprintf(&b, "  %s: %s\n", k, entries[0].Value)
			}
		}
	}
	notes, _ := d.Tasks.Notes(taskID, 10)
	if len(notes) > 0 {
		b.WriteString("\nNotes:\n")
		for _, n := range notes {
			fmt.Fprintf(&b, "- %s%s\n", n.Note, agentSuffix(n.AgentID))
		}
	}
	return b.String()
}

// writeHandoff renders the handoff compactly for LLM consumption.
func writeHandoff(b *strings.Builder, h *task.Handoff) {
	if len(h.ChangedFiles) > 0 {
		fmt.Fprintf(b, "  changed files: %s\n", strings.Join(h.ChangedFiles, ", "))
	}
	writeList := func(label string, items []string) {
		if len(items) == 0 {
			return
		}
		fmt.Fprintf(b, "  %s:\n", label)
		for _, item := range items {
			fmt.Fprintf(b, "    - %s\n", item)
		}
	}
	writeList("details", h.Details)
	writeList("decisions made", h.DecisionsMade)
	writeList("next steps", h.NextSteps)
	if len(h.ContextKeys) > 0 {
		fmt.Fprintf(b, "  context keys: %s\n", strings.Join(h.ContextKeys, ", "))
	}
}

func handleAddTaskNote(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	taskID := str(args, "task_id")
	note := str(args, "note")
	if taskID == "" || note == "" {
		return "error: task_id and note are required"
	}
	id := agentFrom(args, req, "agent_id")
	n, err := d.Tasks.AddNote(taskID, "", id, note)
	if err != nil {
		return "error: " + err.Error()
	}
	d.Hub.Publish(event.TaskNoteAdded, n.ProjectID, map[string]any{
		"task": n.TaskID, "agent": n.AgentID, "note": n.Note,
	})
	return fmt.Sprintf("note #%d added to task %s", n.ID, n.TaskID)
}

func handleGetTaskNotes(d *Deps, req mcp.CallToolRequest) string {
	taskID := str(req.GetArguments(), "task_id")
	if taskID == "" {
		return "error: task_id is required"
	}
	notes, err := d.Tasks.Notes(taskID, 0)
	if err != nil {
		return "error: " + err.Error()
	}
	if len(notes) == 0 {
		return "no notes on task " + taskID
	}
	var b strings.Builder
	for _, n := range notes {
		fmt.Fprintf(&b, "#%d [%s] %s: %s\n", n.ID, n.CreatedAt, orDash(n.AgentID), n.Note)
	}
	return b.String()
}

func taskData(t *task.Task) map[string]any {
	return map[string]any{
		"task_id": t.TaskID, "project_id": t.ProjectID, "title": t.Title,
		"status": t.Status, "assigned_to": t.AssignedTo, "progress": t.Progress,
		"current_activity": t.CurrentActivity,
	}
}

// ---- messaging ----

func messageData(m *message.Message) map[string]any {
	return map[string]any{
		"id": m.ID, "project_id": m.ProjectID, "type": m.Type,
		"from_agent": m.FromAgent, "to_agent": m.ToAgent, "content": m.Content,
		"reply_to": m.ReplyTo, "thread_id": m.ThreadID,
	}
}

// sendOne persists one message and publishes the matching event type.
func sendOne(d *Deps, m message.Message) (*message.Message, error) {
	msg, err := d.Msgs.Send(m)
	if err != nil {
		return nil, err
	}
	typ := event.MessageCreated
	switch m.Type {
	case message.TypeQuestion:
		typ = event.QuestionCreated
	case message.TypeAnswer:
		typ = event.AnswerCreated
	case message.TypeInstruction:
		typ = event.InstructionCreated
	case message.TypeInterfaceChange:
		typ = event.InterfaceChangeSent
	}
	d.Hub.Publish(typ, msg.ProjectID, messageData(msg))
	return msg, nil
}

func handleSendMessage(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	from := agentFrom(args, req, "from_agent")
	if from == "" {
		return "error: from_agent is required"
	}
	proj := d.projectOf(str(args, "project_id"), from)
	if proj == "" {
		return "error: project_id is required (or register your agent with a project)"
	}
	content := str(args, "content")
	if content == "" {
		return "error: content is required"
	}
	typ := str(args, "type")
	if typ == "" {
		typ = message.TypeMessage
	}
	replyTo := int64(num(args, "reply_to"))

	targets := splitList(str(args, "to_agents"))
	if len(targets) == 0 {
		msg, err := sendOne(d, message.Message{
			ProjectID: proj, Type: typ, FromAgent: from,
			ToAgent: message.Broadcast, Content: content, ReplyTo: replyTo,
		})
		if err != nil {
			return "error: " + err.Error()
		}
		return fmt.Sprintf("broadcast #%d sent to project %s", msg.ID, proj)
	}

	sent := []int64{}
	for _, to := range targets {
		msg, err := sendOne(d, message.Message{
			ProjectID: proj, Type: typ, FromAgent: from,
			ToAgent: to, Content: content, ReplyTo: replyTo,
		})
		if err != nil {
			return "error: " + err.Error()
		}
		sent = append(sent, msg.ID)
	}
	return fmt.Sprintf("message(s) %v sent to %s", sent, strings.Join(targets, ", "))
}

func handleAskAgent(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	from := agentFrom(args, req, "from_agent")
	to := str(args, "to_agent")
	if from == "" || to == "" {
		return "error: from_agent and to_agent are required"
	}
	proj := d.projectOf(str(args, "project_id"), from)
	if proj == "" {
		proj = d.projectOf("", to)
	}
	if proj == "" {
		return "error: project_id is required (or register your agent with a project)"
	}
	msg, err := sendOne(d, message.Message{
		ProjectID: proj, Type: message.TypeQuestion,
		FromAgent: from, ToAgent: to, Content: str(args, "question"),
	})
	if err != nil {
		return "error: " + err.Error()
	}
	return fmt.Sprintf("question #%d sent to %s. They will be notified immediately; "+
		"use get_messages(unread_only=true) later to check for an answer.", msg.ID, to)
}

func handleReplyMessage(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	from := agentFrom(args, req, "from_agent")
	parent := int64(num(args, "message_id"))
	if from == "" || parent == 0 {
		return "error: message_id and from_agent are required"
	}
	parentMsg, err := d.Msgs.Get(parent)
	if err != nil {
		return "error: " + err.Error()
	}
	if parentMsg == nil {
		return "error: message to reply to not found"
	}
	typ := str(args, "type")
	if typ == "" {
		typ = message.TypeAnswer
	}
	// A reply is always addressed to the original sender; the thread is
	// inherited from the parent.
	msg, err := d.Msgs.Reply(parent, message.Message{
		Type: typ, FromAgent: from, ToAgent: parentMsg.FromAgent, Content: str(args, "content"),
	})
	if err != nil {
		return "error: " + err.Error()
	}
	typ2 := event.AnswerCreated
	if typ != message.TypeAnswer {
		typ2 = event.MessageCreated
	}
	d.Hub.Publish(typ2, msg.ProjectID, messageData(msg))
	return fmt.Sprintf("reply #%d sent to %s in thread #%d", msg.ID, msg.ToAgent, msg.ThreadID)
}

func handleGetMessages(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	proj := str(args, "project_id")
	agentID := str(args, "agent_id")
	limit := num(args, "limit")
	if limit == 0 {
		limit = 50
	}
	if boolean(args, "unread_only") && agentID != "" {
		// Unread = unacknowledged inbox events of message types.
		rows, err := d.Inbox.Unacked(agentID, limit)
		if err != nil {
			return "error: " + err.Error()
		}
		msgs := []inbox.Event{}
		for _, e := range rows {
			if inbox.IsMessageType(e.Type) {
				msgs = append(msgs, e)
			}
		}
		if len(msgs) == 0 {
			return fmt.Sprintf("no unread messages for %s", agentID)
		}
		var b strings.Builder
		for _, e := range msgs {
			b.WriteString(formatInboxEvent(e))
			b.WriteString("\n")
		}
		return b.String() + fmt.Sprintf("\n%d unread. Acknowledge with mark_read or ack_event after reading.", len(msgs))
	}
	msgs, err := d.Msgs.List(proj, agentID, limit)
	if err != nil {
		return "error: " + err.Error()
	}
	if len(msgs) == 0 {
		return "no messages"
	}
	return formatMessages(msgs)
}

func formatMessages(msgs []message.Message) string {
	var b strings.Builder
	for _, m := range msgs {
		to := m.ToAgent
		if to == "" {
			to = "all"
		}
		reply := ""
		if m.ReplyTo != 0 {
			reply = fmt.Sprintf(" [reply to #%d, thread #%d]", m.ReplyTo, m.ThreadID)
		}
		fmt.Fprintf(&b, "#%d [%s] %s → %s%s: %s\n", m.ID, m.Type, m.FromAgent, to, reply, m.Content)
	}
	return b.String()
}

func handleMarkRead(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	id := agentFrom(args, req, "agent_id")
	if id == "" {
		return "error: agent_id is required"
	}
	n, err := d.Inbox.AckAll(id)
	if err != nil {
		return "error: " + err.Error()
	}
	return fmt.Sprintf("%s marked read: %d inbox event(s) acknowledged", id, n)
}

// ---- inbox (real-time agent protocol) ----

func handleGetUnread(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	id := agentFrom(args, req, "agent_id")
	if id == "" {
		return "error: agent_id is required"
	}
	rows, err := d.Inbox.Unacked(id, num(args, "limit"))
	if err != nil {
		return "error: " + err.Error()
	}
	if len(rows) == 0 {
		return fmt.Sprintf("no unacknowledged events for %s. "+
			"Use wait_for_events(agent_id=%q) to wait for the next one.", id, id)
	}
	var b strings.Builder
	for _, e := range rows {
		b.WriteString(formatInboxEvent(e))
		b.WriteString("\n")
	}
	b.WriteString(fmt.Sprintf("\n%d unacknowledged. Acknowledge with ack_event after processing each one "+
		"(or ack_event without event_id to acknowledge everything).", len(rows)))
	return b.String()
}

func handleAckEvent(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	id := agentFrom(args, req, "agent_id")
	if id == "" {
		return "error: agent_id is required"
	}
	eventID := int64(num(args, "event_id"))
	var (
		n   int64
		err error
	)
	if eventID > 0 {
		n, err = d.Inbox.AckThrough(id, eventID)
	} else {
		n, err = d.Inbox.AckAll(id)
	}
	if err != nil {
		return "error: " + err.Error()
	}
	if eventID > 0 {
		return fmt.Sprintf("acknowledged %d inbox event(s) for %s through event #%d", n, id, eventID)
	}
	return fmt.Sprintf("acknowledged %d inbox event(s) for %s", n, id)
}

func handleWaitForEvents(ctx context.Context, d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	id := agentFrom(args, req, "agent_id")
	if id == "" {
		return "error: agent_id is required"
	}
	timeoutMs := num(args, "timeout_ms")
	if timeoutMs == 0 {
		timeoutMs = 30000
	}
	if timeoutMs < 1000 {
		timeoutMs = 1000
	}
	if timeoutMs > 60000 {
		timeoutMs = 60000
	}
	rows, err := d.Inbox.Wait(ctx, id, time.Duration(timeoutMs)*time.Millisecond)
	if err != nil {
		return "error: " + err.Error()
	}
	if len(rows) == 0 {
		return fmt.Sprintf("timeout: no new events for %s. Call wait_for_events again to keep waiting.", id)
	}
	var b strings.Builder
	for _, e := range rows {
		b.WriteString(formatInboxEvent(e))
		b.WriteString("\n")
	}
	b.WriteString(fmt.Sprintf("\n%d unacknowledged. Acknowledge with ack_event after processing.", len(rows)))
	return b.String()
}

// formatInboxEvent renders one inbox event compactly for LLM consumption.
func formatInboxEvent(e inbox.Event) string {
	data := e.Data
	switch e.Type {
	case inbox.TypeMessage, inbox.TypeQuestion, inbox.TypeAnswer,
		inbox.TypeInstruction, inbox.TypeInterfaceChange:
		to := inbox.DataOf(e, "to_agent")
		if to == "" {
			to = "all"
		}
		return fmt.Sprintf("#%d %s [%s] %s → %s: %s",
			e.EventID, e.Type, e.CreatedAt, orDash(inbox.DataOf(e, "from_agent")), to, inbox.DataOf(e, "content"))
	case inbox.TypeProgress:
		return fmt.Sprintf("#%d progress [%s] %s: task %s at %v%% — %s",
			e.EventID, e.CreatedAt, orDash(inbox.DataOf(e, "agent")), orDash(inbox.DataOf(e, "task_id")),
			data["progress"], orDash(inbox.DataOf(e, "current_activity")))
	case inbox.TypeDecision:
		return fmt.Sprintf("#%d decision [%s] %s: %s",
			e.EventID, e.CreatedAt, orDash(inbox.DataOf(e, "title")), inbox.DataOf(e, "decision"))
	case inbox.TypeTaskCompleted:
		return fmt.Sprintf("#%d task_completed [%s] task %s completed by %s",
			e.EventID, e.CreatedAt, orDash(inbox.DataOf(e, "task_id")), orDash(inbox.DataOf(e, "agent")))
	case inbox.TypeTaskHandoff:
		return fmt.Sprintf("#%d task_handoff [%s] task %s by %s: %s",
			e.EventID, e.CreatedAt, orDash(inbox.DataOf(e, "task")), orDash(inbox.DataOf(e, "agent")),
			inbox.DataOf(e, "summary"))
	}
	return fmt.Sprintf("#%d %s [%s]", e.EventID, e.Type, e.CreatedAt)
}

// ---- shared state ----

func handleProjectActivity(d *Deps, req mcp.CallToolRequest) string {
	proj := str(req.GetArguments(), "project_id")
	if proj == "" {
		return "error: project_id is required"
	}
	return d.activity(proj, 20, 20, true)
}

func handleBriefing(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	proj := str(args, "project_id")
	id := str(args, "agent_id")
	if proj == "" || id == "" {
		return "error: project_id and agent_id are required"
	}
	if sinceID := int64(num(args, "since_event_id")); sinceID > 0 {
		return d.deltaBriefing(proj, id, sinceID)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Project: %s\n\n", proj)

	me, _ := d.Agents.Get(id)
	if me != nil {
		if me.CurrentTask != "" {
			if t, _ := d.Tasks.Get(me.CurrentTask); t != nil {
				fmt.Fprintf(&b, "Your task:\n%s (%s, %d%%%s)\n", t.TaskID, t.Status, t.Progress, suffix(t.CurrentActivity))
				if t.CompletionSummary != "" {
					fmt.Fprintf(&b, "Completion summary: %s\n", t.CompletionSummary)
				}
				if t.Handoff != nil {
					b.WriteString("Handoff:\n")
					writeHandoff(&b, t.Handoff)
				}
				b.WriteString("\n")
			} else {
				fmt.Fprintf(&b, "Your task: %s\n\n", me.CurrentTask)
			}
			b.WriteString(taskNotesSection(d, me.CurrentTask))
		} else {
			b.WriteString("Your task: none claimed yet. Use list_tasks and claim_task.\n\n")
		}
	} else {
		fmt.Fprintf(&b, "Note: agent %s is not registered yet. Call register_agent first.\n\n", id)
	}

	agents, _ := d.Agents.List(proj)
	others := 0
	for _, a := range agents {
		if a.AgentID == id {
			continue
		}
		if a.Status != agent.StatusOnline && a.CurrentTask == "" {
			continue
		}
		others++
		prog, act := agentTaskProgress(d, a)
		fmt.Fprintf(&b, "%s (%s): %s — %d%%%s\n", orDash(a.Name), a.AgentID, orDash(a.CurrentTask), prog, suffix(act))
	}
	if others == 0 {
		b.WriteString("Other active agents: none\n")
	}
	b.WriteString("\n")

	if msgs, err := d.Msgs.List(proj, id, 15); err == nil && len(msgs) > 0 {
		b.WriteString("Recent messages:\n")
		b.WriteString(formatMessages(msgs))
		b.WriteString("\n")
	}

	// Recently completed tasks with handoffs: what was done, without reading
	// the message history. Details via get_task.
	b.WriteString(recentlyCompleted(d, proj))

	if me != nil {
		if n, err := d.Inbox.UnackedCount(id); err == nil && n > 0 {
			fmt.Fprintf(&b, "\nYou have %d unacknowledged inbox event(s). Use get_unread(agent_id=%q) to read them, then ack_event to acknowledge.\n", n, id)
		}
	}

	if decs, err := d.DB.ListDecisions(proj, 10); err == nil {
		if active := activeDecisions(decs); len(active) > 0 {
			b.WriteString("\nImportant decisions:\n")
			b.WriteString(formatDecisions(active))
		}
	}

	if overlaps, err := d.Agents.Overlaps(proj); err == nil && len(overlaps) > 0 {
		b.WriteString("\nFile overlap warnings:\n")
		b.WriteString(formatOverlaps(overlaps))
	}

	if latest, err := d.DB.LatestEventID(proj); err == nil && latest > 0 {
		fmt.Fprintf(&b, "\nLatest event id: %d. On your next briefing pass it as since_event_id to see only what changed.\n", latest)
	}
	return b.String()
}

// deltaBriefing returns only what changed since the given event id, plus the
// agent's current task state — cheap to read after a short absence.
func (d *Deps) deltaBriefing(proj, id string, sinceID int64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Project: %s — changes since event #%d\n\n", proj, sinceID)

	me, _ := d.Agents.Get(id)
	if me != nil && me.CurrentTask != "" {
		if t, _ := d.Tasks.Get(me.CurrentTask); t != nil {
			fmt.Fprintf(&b, "Your task: %s (%s, %d%%%s)\n\n", t.TaskID, t.Status, t.Progress, suffix(t.CurrentActivity))
		}
		if n, err := d.Inbox.UnackedCount(id); err == nil && n > 0 {
			fmt.Fprintf(&b, "You have %d unacknowledged inbox event(s). Use get_unread(agent_id=%q) to read them, then ack_event to acknowledge.\n\n", n, id)
		}
	}

	evs, err := d.Hub.Since(proj, sinceID, 100)
	if err != nil {
		return "error: " + err.Error()
	}
	if len(evs) == 0 {
		b.WriteString("No changes since then.\n")
	} else {
		b.WriteString("Changes:\n")
		for _, ev := range evs {
			fmt.Fprintf(&b, "  #%d %s %s %s\n", ev.ID, ev.CreatedAt, ev.Type, compactData(ev.Data))
		}
		if len(evs) == 100 {
			b.WriteString("  (more changes available — call again with since_event_id = ")
			fmt.Fprintf(&b, "%d)\n", evs[len(evs)-1].ID)
		}
	}

	if latest, err := d.DB.LatestEventID(proj); err == nil && latest > 0 {
		fmt.Fprintf(&b, "\nLatest event id: %d.\n", latest)
	}
	return b.String()
}

// recentlyCompleted summarizes the most recent completed tasks that carry a
// completion summary or handoff, newest first. Informational only.
func recentlyCompleted(d *Deps, proj string) string {
	done, err := d.Tasks.List(proj, task.StatusCompleted, "")
	if err != nil {
		return ""
	}
	sort.Slice(done, func(i, j int) bool { return done[i].UpdatedAt > done[j].UpdatedAt })
	lines := []string{}
	for _, t := range done {
		if t.CompletionSummary == "" && t.Handoff == nil {
			continue
		}
		summary := t.CompletionSummary
		if summary == "" && t.Handoff != nil {
			summary = t.Handoff.Summary
		}
		if len(summary) > 100 {
			summary = summary[:97] + "..."
		}
		lines = append(lines, fmt.Sprintf("- %s: %s (details: get_task(task_id=%q))\n", t.TaskID, summary, t.TaskID))
		if len(lines) == 3 {
			break
		}
	}
	if len(lines) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("Recently completed:\n")
	b.WriteString(strings.Join(lines, ""))
	b.WriteString("\n")
	return b.String()
}

// taskNotesSection renders the notes on a task, or "" when there are none.
func taskNotesSection(d *Deps, taskID string) string {
	notes, err := d.Tasks.Notes(taskID, 10)
	if err != nil || len(notes) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Notes on task %s:\n", taskID)
	for _, n := range notes {
		fmt.Fprintf(&b, "- %s%s\n", n.Note, agentSuffix(n.AgentID))
	}
	b.WriteString("\n")
	return b.String()
}

// activeDecisions filters out decisions that a newer one has replaced.
func activeDecisions(decs []db.Decision) []db.Decision {
	out := make([]db.Decision, 0, len(decs))
	for _, dec := range decs {
		if dec.SupersededBy == 0 {
			out = append(out, dec)
		}
	}
	return out
}

func formatDecisions(decs []db.Decision) string {
	var b strings.Builder
	for _, dec := range decs {
		fmt.Fprintf(&b, "- %s: %s", dec.Title, dec.Decision)
		if dec.Reason != "" {
			fmt.Fprintf(&b, " (%s)", dec.Reason)
		}
		if dec.SupersededBy != 0 {
			fmt.Fprintf(&b, " [SUPERSEDED by #%d]", dec.SupersededBy)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func agentSuffix(agentID string) string {
	if agentID == "" {
		return ""
	}
	return " (" + agentID + ")"
}

func agentTaskProgress(d *Deps, a agent.Agent) (int, string) {
	if a.CurrentTask == "" {
		return 0, ""
	}
	if t, err := d.Tasks.Get(a.CurrentTask); err == nil && t != nil {
		return t.Progress, t.CurrentActivity
	}
	return 0, ""
}

func (d *Deps) activity(proj string, eventsLimit, msgLimit int, withEvents bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Project: %s\n\n", proj)

	agents, _ := d.Agents.List(proj)
	b.WriteString("Agents:\n")
	if len(agents) == 0 {
		b.WriteString("  none\n")
	}
	for _, a := range agents {
		prog, act := agentTaskProgress(d, a)
		fmt.Fprintf(&b, "  %s (%s) [%s] task=%s %d%% %s files=%s\n",
			orDash(a.Name), a.AgentID, a.Status, orDash(a.CurrentTask), prog, suffix(act),
			orDash(strings.Join(a.Files, ", ")))
	}

	tasks, _ := d.Tasks.List(proj, "", "")
	b.WriteString("\nTasks:\n")
	if len(tasks) == 0 {
		b.WriteString("  none\n")
	}
	for _, t := range tasks {
		fmt.Fprintf(&b, "  %s [%s] %s — %d%% %s (%s)\n",
			t.TaskID, t.Status, orDash(t.Title), t.Progress, suffix(t.CurrentActivity), orDash(t.AssignedTo))
	}

	if overlaps, _ := d.Agents.Overlaps(proj); len(overlaps) > 0 {
		b.WriteString("\nFile overlaps:\n")
		b.WriteString(formatOverlaps(overlaps))
	}

	if decs, _ := d.DB.ListDecisions(proj, 10); len(decs) > 0 {
		b.WriteString("\nRecent decisions:\n")
		b.WriteString(formatDecisions(activeDecisions(decs)))
	}

	if withEvents {
		if evs, err := d.Hub.Recent(proj, eventsLimit); err == nil && len(evs) > 0 {
			b.WriteString("\nRecent events:\n")
			for _, ev := range evs {
				fmt.Fprintf(&b, "  %s %s %s\n", ev.CreatedAt, ev.Type, compactData(ev.Data))
			}
		}
	}
	return b.String()
}

func handleRecordDecision(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	proj := str(args, "project_id")
	title := str(args, "title")
	decision := str(args, "decision")
	if proj == "" || title == "" || decision == "" {
		return "error: project_id, title and decision are required"
	}
	dec, err := d.DB.RecordDecision(db.Decision{
		ProjectID:  proj,
		Title:      title,
		Decision:   decision,
		Reason:     str(args, "reason"),
		AgentID:    str(args, "agent_id"),
		Supersedes: int64(num(args, "supersedes")),
	})
	if err != nil {
		return "error: " + err.Error()
	}
	d.Hub.Publish(event.DecisionCreated, proj, map[string]any{
		"id": dec.ID, "title": dec.Title, "decision": dec.Decision, "reason": dec.Reason,
		"agent": dec.AgentID, "supersedes": dec.Supersedes,
	})
	if dec.Supersedes != 0 {
		return fmt.Sprintf("decision #%d recorded: %s (supersedes #%d — briefings now show only the new decision)",
			dec.ID, dec.Title, dec.Supersedes)
	}
	return fmt.Sprintf("decision #%d recorded: %s", dec.ID, dec.Title)
}

func handleGetDecisions(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	proj := str(args, "project_id")
	if proj == "" {
		return "error: project_id is required"
	}
	decs, err := d.DB.ListDecisions(proj, num(args, "limit"))
	if err != nil {
		return "error: " + err.Error()
	}
	if boolean(args, "active_only") {
		decs = activeDecisions(decs)
	}
	if len(decs) == 0 {
		return "no decisions recorded for " + proj
	}
	var b strings.Builder
	for _, dec := range decs {
		fmt.Fprintf(&b, "#%d %s\n  decision: %s\n", dec.ID, dec.Title, dec.Decision)
		if dec.Reason != "" {
			fmt.Fprintf(&b, "  reason: %s\n", dec.Reason)
		}
		if dec.AgentID != "" {
			fmt.Fprintf(&b, "  by: %s\n", dec.AgentID)
		}
		if dec.Supersedes != 0 {
			fmt.Fprintf(&b, "  supersedes: #%d\n", dec.Supersedes)
		}
		if dec.SupersededBy != 0 {
			fmt.Fprintf(&b, "  SUPERSEDED by: #%d\n", dec.SupersededBy)
		}
	}
	return b.String()
}

func handleSetContext(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	proj := str(args, "project_id")
	key := str(args, "key")
	value := str(args, "value")
	if proj == "" || key == "" {
		return "error: project_id and key are required"
	}
	if err := d.DB.SetContext(proj, key, value); err != nil {
		return "error: " + err.Error()
	}
	return fmt.Sprintf("context %s/%s set", proj, key)
}

func handleGetContext(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	proj := str(args, "project_id")
	if proj == "" {
		return "error: project_id is required"
	}
	entries, err := d.DB.GetContext(proj, str(args, "key"))
	if err != nil {
		return "error: " + err.Error()
	}
	if len(entries) == 0 {
		return "no context stored for " + proj
	}
	var b strings.Builder
	for _, e := range entries {
		fmt.Fprintf(&b, "%s: %s\n", e.Key, e.Value)
	}
	return b.String()
}

func handleListContext(d *Deps, req mcp.CallToolRequest) string {
	args := req.GetArguments()
	proj := str(args, "project_id")
	if proj == "" {
		return "error: project_id is required"
	}
	entries, err := d.DB.SearchContext(proj, str(args, "prefix"), str(args, "search"), num(args, "limit"))
	if err != nil {
		return "error: " + err.Error()
	}
	if len(entries) == 0 {
		return "no context matches for " + proj
	}
	var b strings.Builder
	for _, e := range entries {
		fmt.Fprintf(&b, "%s: %s (updated %s)\n", e.Key, e.Value, e.UpdatedAt)
	}
	return b.String()
}

// ---- formatting helpers ----

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func suffix(s string) string {
	if s == "" {
		return ""
	}
	return " — " + s
}

func splitList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func compactData(data map[string]any) string {
	skip := map[string]bool{"files": true}
	parts := []string{}
	for k, v := range data {
		if skip[k] {
			continue
		}
		s, ok := v.(string)
		if !ok || s == "" {
			continue
		}
		if len(s) > 60 {
			s = s[:57] + "..."
		}
		parts = append(parts, k+"="+s)
	}
	if len(parts) == 0 {
		return ""
	}
	return "(" + strings.Join(parts, ", ") + ")"
}
