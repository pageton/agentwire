// Package task manages tasks. Tasks are informational coordination units:
// they never block each other. A task in status "pending" can be claimed and
// worked on immediately, even if a conceptually related task is unfinished.
package task

import (
	"database/sql"
	"errors"
	"fmt"

	"agentwire/internal/db"
)

// Task statuses.
const (
	StatusPending   = "pending"
	StatusWorking   = "working"
	StatusBlocked   = "blocked"
	StatusCompleted = "completed"
)

// Task represents a piece of work performed by an agent.
type Task struct {
	TaskID          string `json:"task_id"`
	ProjectID       string `json:"project_id"`
	Title           string `json:"title"`
	Description     string `json:"description,omitempty"`
	Status          string `json:"status"`
	AssignedTo      string `json:"assigned_to,omitempty"`
	CreatedBy       string `json:"created_by,omitempty"`
	Progress        int    `json:"progress"`
	CurrentActivity string `json:"current_activity,omitempty"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

// Store provides task persistence.
type Store struct {
	DB *db.Store
}

func NewStore(database *db.Store) *Store { return &Store{DB: database} }

// Create inserts a new task. Dependencies are informational only and are
// expressed via description/messages, never via execution gating.
func (s *Store) Create(t Task) error {
	if t.Status == "" {
		t.Status = StatusPending
	}
	now := db.Now()
	if t.CreatedAt == "" {
		t.CreatedAt = now
	}
	t.UpdatedAt = now
	_, err := s.DB.DB.Exec(
		`INSERT INTO tasks (task_id, project_id, title, description, status, assigned_to, created_by, progress, current_activity, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.TaskID, t.ProjectID, t.Title, t.Description, t.Status, t.AssignedTo, t.CreatedBy, t.Progress, t.CurrentActivity, t.CreatedAt, t.UpdatedAt,
	)
	return err
}

// Get returns the task or nil when unknown.
func (s *Store) Get(taskID string) (*Task, error) {
	row := s.DB.DB.QueryRow(
		`SELECT task_id, project_id, title, description, status, assigned_to, created_by, progress, current_activity, created_at, updated_at
		 FROM tasks WHERE task_id = ?`,
		taskID,
	)
	t, err := scanTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

// List returns tasks filtered by project, status and/or assignee.
func (s *Store) List(projectID, status, assignedTo string) ([]Task, error) {
	query := `SELECT task_id, project_id, title, description, status, assigned_to, created_by, progress, current_activity, created_at, updated_at
	          FROM tasks WHERE 1=1`
	var args []any
	if projectID != "" {
		query += ` AND project_id = ?`
		args = append(args, projectID)
	}
	if status != "" {
		query += ` AND status = ?`
		args = append(args, status)
	}
	if assignedTo != "" {
		query += ` AND assigned_to = ?`
		args = append(args, assignedTo)
	}
	query += ` ORDER BY task_id`
	rows, err := s.DB.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Task{}
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

// Claim assigns the task to an agent and marks it working. Claiming never
// waits for other tasks — parallel work is the default.
func (s *Store) Claim(taskID, agentID string) (*Task, error) {
	res, err := s.DB.DB.Exec(
		`UPDATE tasks SET status = ?, assigned_to = ?, updated_at = ? WHERE task_id = ?`,
		StatusWorking, agentID, db.Now(), taskID,
	)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, fmt.Errorf("task %q not found", taskID)
	}
	return s.Get(taskID)
}

// UpdateProgress updates progress (clamped to 0-100), the human-readable
// activity, and optionally the status (e.g. blocked).
func (s *Store) UpdateProgress(taskID string, progress int, activity, status string) (*Task, error) {
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}
	if status == "" {
		_, err := s.DB.DB.Exec(
			`UPDATE tasks SET progress = ?, current_activity = ?, updated_at = ? WHERE task_id = ?`,
			progress, activity, db.Now(), taskID,
		)
		if err != nil {
			return nil, err
		}
		return s.Get(taskID)
	}
	if status != StatusPending && status != StatusWorking && status != StatusBlocked && status != StatusCompleted {
		return nil, fmt.Errorf("invalid task status %q", status)
	}
	_, err := s.DB.DB.Exec(
		`UPDATE tasks SET progress = ?, current_activity = ?, status = ?, updated_at = ? WHERE task_id = ?`,
		progress, activity, status, db.Now(), taskID,
	)
	if err != nil {
		return nil, err
	}
	return s.Get(taskID)
}

// Complete marks the task completed with progress 100.
func (s *Store) Complete(taskID string) (*Task, error) {
	res, err := s.DB.DB.Exec(
		`UPDATE tasks SET status = ?, progress = 100, updated_at = ? WHERE task_id = ?`,
		StatusCompleted, db.Now(), taskID,
	)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, fmt.Errorf("task %q not found", taskID)
	}
	return s.Get(taskID)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTask(row rowScanner) (*Task, error) {
	var t Task
	err := row.Scan(&t.TaskID, &t.ProjectID, &t.Title, &t.Description, &t.Status, &t.AssignedTo, &t.CreatedBy, &t.Progress, &t.CurrentActivity, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// CountsByStatus returns the number of tasks per status for a project
// (or across all projects when projectID is empty).
func (s *Store) CountsByStatus(projectID string) (map[string]int, error) {
	query := `SELECT status, COUNT(*) FROM tasks`
	var args []any
	if projectID != "" {
		query += ` WHERE project_id = ?`
		args = append(args, projectID)
	}
	query += ` GROUP BY status`
	rows, err := s.DB.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}
