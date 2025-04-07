package models

import (
	"time"

	"github.com/guregu/null/v6"
)

// This file contains all the models under the `tasks` schema

// TaskDefinition is a models representing the `tasks.definition` table
type TaskDefinition struct {
	ID             int64       `db:"id"`
	Name           string      `db:"name"`
	Description    null.String `db:"description"`
	ImageName      null.String `db:"image_name"`
	Command        string      `db:"command"`
	TimeoutSeconds int64       `db:"timeout_seconds"`
	MaxRetries     int         `db:"max_retries"`
	CreatedAt      time.Time   `db:"created_at"`
	UpdatedAt      time.Time   `db:"updated_at"`
	IsActive       bool        `db:"is_active"`
}

type RequiredCondition string

const (
	RcSuccess    RequiredCondition = "success"
	RcCompletion RequiredCondition = "completion"
	RcFailure    RequiredCondition = "failure"
	RcCancelled  RequiredCondition = "cancelled"
	RcLapsed     RequiredCondition = "lapsed"
)

// TaskDependency is a models representing the `tasks.dependency` table
type TaskDependency struct {
	ID                int64             `db:"id"`
	TaskID            int64             `db:"task_id"`
	DependsOn         int64             `db:"depends_on"`
	LookbackWindow    int64             `db:"lookback_window"`
	MinWaitTime       int64             `db:"min_wait_time"`
	RequiredCondition RequiredCondition `db:"required_condition"`
	CreatedAt         time.Time         `db:"created_at"`
	UpdatedAt         time.Time         `db:"updated_at"`
}

// TaskSchedule is a models representing the `tasks.schedule` table
type TaskSchedule struct {
	ID             int64     `db:"id"`
	TaskID         int64     `db:"task_id"`
	CronExpression string    `db:"cron_expression"`
	Timezone       string    `db:"timezone"`
	CreatedAt      time.Time `db:"created_at"`
	UpdatedAt      time.Time `db:"updated_at"`
}

type RunStatus string

const (
	RsPending   RunStatus = "pending"
	RsRunning   RunStatus = "running"
	RsCompleted RunStatus = "completed"
	RsFailed    RunStatus = "failed"
	RsCancelled RunStatus = "cancelled"
	RsLapsed    RunStatus = "lapsed"
)

type TaskRun struct {
	ID            int64       `db:"id"`
	TaskID        int64       `db:"task_id"`
	Status        RunStatus   `db:"status"`
	StartTime     null.Time   `db:"start_time"`
	EndTime       null.Time   `db:"end_time"`
	ExitCode      int         `db:"exit_code"`
	Output        null.String `db:"output"`
	Error         null.String `db:"error"`
	Attempts      int         `db:"attempts"`
	WorkerId      null.String `db:"worker_id"`
	LastHeartbeat null.Time   `db:"last_heartbeat"`
	CreatedAt     time.Time   `db:"created_at"`
}
