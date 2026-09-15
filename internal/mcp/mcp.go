// Package mcp exposes AgentWire as an MCP server. All tools are plain
// coordination primitives: they read/write shared state and emit events.
// AgentWire never executes code or controls an LLM.
package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"agentwire/internal/agent"
	"agentwire/internal/db"
	"agentwire/internal/event"
	"agentwire/internal/message"
	"agentwire/internal/task"
)

const serverVersion = "0.1.0"

// Deps bundles the stores and hub the tools operate on.
type Deps struct {
	DB     *db.Store
	Hub    *event.Hub
	Agents *agent.Store
	Tasks  *task.Store
	Msgs   *message.Store
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
			"are notified."),
		mcp.WithString("task_id", mcp.Description("Task id to complete"), mcp.Required()),
		mcp.WithString("agent_id", mcp.Description("Your agent id"), mcp.Required()),
	), withDeps(deps, handleCompleteTask))

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
			"(or broadcast) that you have not seen yet — call mark_read afterwards to dismiss them. "+
			"With unread_only=false returns recent project history."),
		mcp.WithString("project_id", mcp.Description("Project id (optional, all projects if omitted)")),
		mcp.WithString("agent_id", mcp.Description("Agent id whose inbox to read (optional)")),
		mcp.WithBoolean("unread_only", mcp.Description("Only messages not yet delivered to the agent (default false)")),
		mcp.WithNumber("limit", mcp.Description("Max messages (default 50)")),
	), withDeps(deps, handleGetMessages))

	s.AddTool(mcp.NewTool("mark_read",
		mcp.WithDescription("Mark messages as seen by your agent: everything up to 'message_id' is "+
			"considered delivered, or everything when message_id is omitted. Useful after reading "+
			"unread messages via get_messages."),
		mcp.WithString("agent_id", mcp.Description("Your agent id"), mcp.Required()),
		mcp.WithNumber("message_id", mcp.Description("Mark all messages up to and including this id as read (optional)")),
	), withDeps(deps, handleMarkRead))

	// ---- shared state ----
	s.AddTool(mcp.NewTool("get_project_activity",
		mcp.WithDescription("Get a complete live picture of a project: all agents with presence and files, "+
			"all tasks with progress and activity, file-overlap warnings, recent events and recent decisions."),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
	), withDeps(deps, handleProjectActivity))

	s.AddTool(mcp.NewTool("get_briefing",
		mcp.WithDescription("Get a compact startup briefing for your agent: your task, other active agents "+
			"with their progress, recent messages, unread messages and important decisions. Call this when "+
			"you (re)start a session to immediately understand what the team is doing."),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
		mcp.WithString("agent_id", mcp.Description("Your agent id"), mcp.Required()),
	), withDeps(deps, handleBriefing))

	s.AddTool(mcp.NewTool("record_decision",
		mcp.WithDescription("Record an important implementation decision for the project so other agents "+
			"don't ask the same question twice. Include the reason — it helps teammates adapt."),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
		mcp.WithString("title", mcp.Description("Short decision title, e.g. 'RecovererEnv clock injection'"), mcp.Required()),
		mcp.WithString("decision", mcp.Description("What was decided"), mcp.Required()),
		mcp.WithString("reason", mcp.Description("Why — e.g. 'Makes decision-table tests deterministic'")),
		mcp.WithString("agent_id", mcp.Description("Your agent id (optional)")),
	), withDeps(deps, handleRecordDecision))

	s.AddTool(mcp.NewTool("get_decisions",
		mcp.WithDescription("Retrieve recorded implementation decisions for a project."),
		mcp.WithString("project_id", mcp.Description("Project id"), mcp.Required()),
		mcp.WithNumber("limit", mcp.Description("Max decisions (default 20)")),
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
	typ := event.TaskProgress
	if t.Status == task.StatusBlocked {
		typ = event.TaskBlocked
	} else if t.Status == task.StatusCompleted {
		typ = event.TaskCompleted
	}
	d.Hub.Publish(typ, t.ProjectID, taskData(t))
	d.Hub.Publish(typ, t.ProjectID, map[string]any{
		"agent": id, "task": t.TaskID, "progress": t.Progress, "activity": t.CurrentActivity,
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
	t, err := d.Tasks.Complete(taskID)
	if err != nil {
		return "error: " + err.Error()
	}
	d.Hub.Publish(event.TaskCompleted, t.ProjectID, taskData(t))
	d.Hub.Publish(event.TaskCompleted, t.ProjectID, map[string]any{
		"agent": id, "task": t.TaskID,
	})
	return fmt.Sprintf("task %s completed by %s. Other agents in %s have been notified.", taskID, id, t.ProjectID)
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
		if proj == "" {
			proj = d.projectOf("", agentID)
		}
		a, err := d.Agents.Get(agentID)
		if err != nil || a == nil {
			return "error: unknown agent " + agentID
		}
		msgs, err := d.Msgs.Unread(agentID, proj, a.LastMsgID, limit)
		if err != nil {
			return "error: " + err.Error()
		}
		if len(msgs) == 0 {
			return fmt.Sprintf("no unread messages for %s", agentID)
		}
		return formatMessages(msgs) + fmt.Sprintf("\n%d unread. Call mark_read after reading.", len(msgs))
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
	msgID := int64(num(args, "message_id"))
	if msgID == 0 {
		if max, err := d.Msgs.MaxID(); err == nil {
			msgID = max
		}
	}
	if err := d.Agents.SetCursor(id, msgID); err != nil {
		return "error: " + err.Error()
	}
	return fmt.Sprintf("%s marked read through message #%d", id, msgID)
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
	var b strings.Builder
	fmt.Fprintf(&b, "Project: %s\n\n", proj)

	me, _ := d.Agents.Get(id)
	if me != nil {
		if me.CurrentTask != "" {
			if t, _ := d.Tasks.Get(me.CurrentTask); t != nil {
				fmt.Fprintf(&b, "Your task:\n%s (%s, %d%%%s)\n\n", t.TaskID, t.Status, t.Progress, suffix(t.CurrentActivity))
			} else {
				fmt.Fprintf(&b, "Your task: %s\n\n", me.CurrentTask)
			}
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

	if me != nil {
		if unread, err := d.Msgs.Unread(id, proj, me.LastMsgID, 100); err == nil && len(unread) > 0 {
			fmt.Fprintf(&b, "\nYou have %d unread message(s). Use get_messages(unread_only=true, agent_id=%q) to read them.\n", len(unread), id)
		}
	}

	if decs, err := d.DB.ListDecisions(proj, 10); err == nil && len(decs) > 0 {
		b.WriteString("\nImportant decisions:\n")
		for _, dec := range decs {
			fmt.Fprintf(&b, "- %s: %s", dec.Title, dec.Decision)
			if dec.Reason != "" {
				fmt.Fprintf(&b, " (%s)", dec.Reason)
			}
			b.WriteString("\n")
		}
	}

	if overlaps, err := d.Agents.Overlaps(proj); err == nil && len(overlaps) > 0 {
		b.WriteString("\nFile overlap warnings:\n")
		b.WriteString(formatOverlaps(overlaps))
	}
	return b.String()
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
		for _, dec := range decs {
			fmt.Fprintf(&b, "  - %s: %s\n", dec.Title, dec.Decision)
		}
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
		ProjectID: proj,
		Title:     title,
		Decision:  decision,
		Reason:    str(args, "reason"),
		AgentID:   str(args, "agent_id"),
	})
	if err != nil {
		return "error: " + err.Error()
	}
	d.Hub.Publish(event.DecisionCreated, proj, map[string]any{
		"id": dec.ID, "title": dec.Title, "decision": dec.Decision, "reason": dec.Reason, "agent": dec.AgentID,
	})
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
