package main

import (
	"encoding/json"
	"fmt"
	"time"
)

type TaskState string

const (
	StatePending    TaskState = "PENDING"
	StateProcessing TaskState = "PROCESSING"
	StateCompleted  TaskState = "COMPLETED"
	StateFailed     TaskState = "FAILED"
	StateDLQ        TaskState = "DLQ"
)

type Task struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"` // Raw JSON deferred until handler execution
	State      TaskState       `json:"state"`
	RetryCount int             `json:"retry_count"`
	MaxRetries int             `json:"max_retries"`
	Error      string          `json:"error,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

func NewTask(id, taskType string, payload []byte) *Task {
	now := time.Now().UTC()
	return &Task{
		ID:         id,
		Type:       taskType,
		Payload:    json.RawMessage(payload),
		State:      StatePending,
		RetryCount: 0,
		MaxRetries: 3,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

func (t *Task) Marshal() ([]byte, error) {
	return json.Marshal(t)
}

func Unmarshal(data []byte) (*Task, error) {
	var task Task
	err := json.Unmarshal(data, &task)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}
	return &task, nil
}
