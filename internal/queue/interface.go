package queue

import (
	"context"
	"time"
)

// TaskMessage represents a message sent to the queue
type TaskMessage struct {
	ExecutionID int64     `json:"execution_id"`
	JobID       int64     `json:"job_id"`
	Command     string    `json:"command"`
	ImageName   string    `json:"image_name,omitempty"`
	Timeout     int       `json:"timeout_seconds"`
	MaxRetries  int       `json:"max_retries"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

// Client defines the interface for task queue operations
type Client interface {
	Publish(ctx context.Context, message TaskMessage) error
	Subscribe(ctx context.Context, handler func(TaskMessage) error) error
	Close() error
}
