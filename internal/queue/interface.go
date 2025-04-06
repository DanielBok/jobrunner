package queue

import (
	"context"
	"time"
)

// TaskMessage represents a message sent to the queue
type TaskMessage struct {
	ExecutionID int64     `json:"execution_id"`
	TaskID      int64     `json:"task_id"`
	Command     string    `json:"command"`
	ImageName   string    `json:"image_name,omitempty"`
	Timeout     int       `json:"timeout_seconds"`
	MaxRetries  int       `json:"max_retries"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

// Client defines the interface for task queue operations
type Client interface {
	// Publish sends a task message to the queue
	Publish(ctx context.Context, message TaskMessage) error
	// Subscribe starts listening for messages and processes them with the handler. One client can only be subscribed once
	Subscribe(ctx context.Context, handler func(TaskMessage) error) error
	// Close terminates the Client connection
	Close() error
}
