// Command agentwire is the AgentWire server and CLI: a small, fast shared
// real-time communication channel for multiple AI coding agents working in
// parallel on the same project.
//
//	agentwire start     start the MCP + WebSocket server
//	agentwire status    overview of the bus
//	agentwire agents    list agents
//	agentwire tasks     list tasks
//	agentwire activity  recent events
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mark3labs/mcp-go/server"

	"agentwire/internal/agent"
	"agentwire/internal/db"
	"agentwire/internal/event"
	"agentwire/internal/inbox"
	agentmcp "agentwire/internal/mcp"
	"agentwire/internal/message"
	"agentwire/internal/task"
	agentws "agentwire/internal/websocket"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "start":
		err = runStart()
	case "status":
		err = runStatus()
	case "agents":
		err = runAgents(os.Args[2:])
	case "tasks":
		err = runTasks(os.Args[2:])
	case "activity":
		err = runActivity(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Print(`AgentWire — real-time collaboration bus for AI coding agents

usage:
  agentwire start            start the MCP + WebSocket server
  agentwire status           overview: db, addr, counts
  agentwire agents [project] list agents
  agentwire tasks [project]  list tasks
  agentwire activity [project] [n]  recent events (n defaults to 30)

configuration (environment):
  AGENTWIRE_ADDR   listen address, default ":8080"
  AGENTWIRE_DB     sqlite file, default "./agentwire.db"
  AGENTWIRE_TOKEN  shared auth token, default "" (auth disabled)

endpoints:
  MCP (streamable HTTP): POST http://<addr>/mcp
  MCP (legacy SSE):      GET  http://<addr>/sse
  WebSocket events:      ws://<addr>/ws?agent_id=agent-1&token=<token>
  Health:                GET  http://<addr>/healthz
`)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ---- start ----

func runStart() error {
	addr := envOr("AGENTWIRE_ADDR", ":8080")
	dbPath := envOr("AGENTWIRE_DB", "./agentwire.db")
	token := envOr("AGENTWIRE_TOKEN", "")

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	database, err := db.Open(dbPath)
	if err != nil {
		return err
	}
	defer database.Close()

	agents := agent.NewStore(database)
	tasks := task.NewStore(database)
	msgs := message.NewStore(database)
	inbx := inbox.NewStore(database)
	hub := event.NewHub(database)
	hub.SetInbox(inbx)

	wsServer := agentws.NewServer(database, hub, agents, tasks, msgs, inbx, token)
	mcpServer := agentmcp.New(agentmcp.Deps{
		DB: database, Hub: hub, Agents: agents, Tasks: tasks, Msgs: msgs, Inbox: inbx,
	})

	streamable := server.NewStreamableHTTPServer(mcpServer,
		server.WithHeartbeatInterval(30*time.Second),
	)
	sse := server.NewSSEServer(mcpServer,
		server.WithBaseURL(baseURL(addr)),
	)

	mux := http.NewServeMux()
	mux.Handle("/ws", wsServer)
	mux.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok\n")
	}))
	mux.Handle("/mcp", requireToken(token, streamable))
	mux.Handle("/sse", requireToken(token, sse))
	mux.Handle("/message", requireToken(token, sse))

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           logRequests(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		slog.Info("AgentWire started",
			"addr", addr, "db", dbPath, "mcp", "/mcp", "ws", "/ws",
			"auth", token != "")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server failed", "err", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	slog.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return httpServer.Shutdown(ctx)
}

func baseURL(addr string) string {
	host := addr
	if strings.HasPrefix(addr, ":") {
		host = "localhost" + addr
	}
	return "http://" + host
}

func requireToken(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" {
			next.ServeHTTP(w, r)
			return
		}
		got := r.Header.Get("Authorization")
		if strings.HasPrefix(got, "Bearer ") {
			got = strings.TrimPrefix(got, "Bearer ")
		}
		if got == "" {
			got = r.URL.Query().Get("token")
		}
		if got != token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if r.URL.Path != "/healthz" {
			slog.Debug("http", "method", r.Method, "path", r.URL.Path, "dur", time.Since(start))
		}
	})
}

// ---- status / agents / tasks / activity ----

func openDB() (*db.Store, error) {
	return db.Open(envOr("AGENTWIRE_DB", "./agentwire.db"))
}

func runStatus() error {
	database, err := openDB()
	if err != nil {
		return err
	}
	defer database.Close()

	agents, online, nTasks, nMsgs, nDecs, nEvents, unacked, err := database.Counts()
	if err != nil {
		return err
	}
	tasks := task.NewStore(database)
	byStatus, err := tasks.CountsByStatus("")
	if err != nil {
		return err
	}

	addr := envOr("AGENTWIRE_ADDR", ":8080")
	conn, err := net.DialTimeout("tcp", listenHost(addr), 500*time.Millisecond)
	running := "not running"
	if err == nil {
		conn.Close()
		running = "running"
	}

	fmt.Printf("AgentWire status\n")
	fmt.Printf("  db:       %s\n", envOr("AGENTWIRE_DB", "./agentwire.db"))
	fmt.Printf("  addr:     %s\n", addr)
	fmt.Printf("  server:   %s\n", running)
	fmt.Printf("  agents:   %d (%d online)\n", agents, online)
	fmt.Printf("  tasks:    %d (%s)\n", nTasks, statusSummary(byStatus))
	fmt.Printf("  messages: %d\n", nMsgs)
	fmt.Printf("  decisions: %d\n", nDecs)
	fmt.Printf("  events:   %d\n", nEvents)
	fmt.Printf("  inbox:    %d unacknowledged\n", unacked)
	return nil
}

func listenHost(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "localhost" + addr
	}
	return addr
}

func statusSummary(m map[string]int) string {
	parts := []string{}
	for _, st := range []string{task.StatusPending, task.StatusWorking, task.StatusBlocked, task.StatusCompleted} {
		if n, ok := m[st]; ok && n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, st))
		}
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

func runAgents(args []string) error {
	database, err := openDB()
	if err != nil {
		return err
	}
	defer database.Close()

	proj := ""
	if len(args) > 0 {
		proj = args[0]
	}
	agents, err := agent.NewStore(database).List(proj)
	if err != nil {
		return err
	}
	if len(agents) == 0 {
		fmt.Println("no agents")
		return nil
	}
	fmt.Printf("%-16s %-12s %-8s %-10s %-12s %s\n", "AGENT_ID", "NAME", "STATUS", "PROJECT", "TASK", "FILES")
	for _, a := range agents {
		fmt.Printf("%-16s %-12s %-8s %-10s %-12s %s\n",
			a.AgentID, truncate(a.Name, 12), a.Status, a.ProjectID,
			truncate(a.CurrentTask, 12), truncate(strings.Join(a.Files, ", "), 60))
	}
	return nil
}

func runTasks(args []string) error {
	database, err := openDB()
	if err != nil {
		return err
	}
	defer database.Close()

	proj := ""
	if len(args) > 0 {
		proj = args[0]
	}
	tasks, err := task.NewStore(database).List(proj, "", "")
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		fmt.Println("no tasks")
		return nil
	}
	fmt.Printf("%-12s %-10s %-10s %-12s %-9s %s\n", "TASK_ID", "PROJECT", "STATUS", "ASSIGNED", "PROGRESS", "ACTIVITY")
	for _, t := range tasks {
		fmt.Printf("%-12s %-10s %-10s %-12s %-9s %s\n",
			t.TaskID, t.ProjectID, t.Status, truncate(t.AssignedTo, 12),
			fmt.Sprintf("%d%%", t.Progress), truncate(t.CurrentActivity, 50))
	}
	return nil
}

func runActivity(args []string) error {
	database, err := openDB()
	if err != nil {
		return err
	}
	defer database.Close()

	proj := ""
	limit := 30
	if len(args) > 0 {
		proj = args[0]
	}
	if len(args) > 1 {
		fmt.Sscanf(args[1], "%d", &limit)
	}
	rows, err := database.RecentEvents(proj, limit)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fmt.Println("no events")
		return nil
	}
	fmt.Printf("%-21s %-22s %-10s %s\n", "TIME", "TYPE", "PROJECT", "DATA")
	for _, r := range rows {
		fmt.Printf("%-21s %-22s %-10s %s\n",
			r.CreatedAt[:min(len(r.CreatedAt), 19)], r.Type, r.ProjectID, truncate(r.Data, 70))
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 3 {
		return s[:n]
	}
	return s[:n-3] + "..."
}
