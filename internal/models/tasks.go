package models

import (
	"time"

	"github.com/guregu/null/v6"
)

// This file contains all the models under the `tasks` schema

// TasksJob is a models representing the `tasks.job` table
type TasksJob struct {
	ID             int64       `db:"id"`
	Name           string      `db:"name"`
	Description    null.String `db:"description"`
	ImageName      null.String `db:"image_name"`
	Command        string      `db:"command"`
	TimeoutSeconds int         `db:"timeout_seconds"`
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

// TasksDependency is a models representing the `tasks.dependency` table
type TasksDependency struct {
	ID                int64             `db:"id"`
	JobID             int64             `db:"job_id"`
	DependsOn         int64             `db:"depends_on"`
	LookbackWindow    int               `db:"lookback_window"`
	MinWaitTime       int               `db:"min_wait_time"`
	RequiredCondition RequiredCondition `db:"required_condition"`
	CreatedAt         time.Time         `db:"created_at"`
	UpdatedAt         time.Time         `db:"updated_at"`
}

// TasksSchedule is a models representing the `tasks.schedule` table
type TasksSchedule struct {
	ID             int64     `db:"id"`
	JobID          int64     `db:"job_id"`
	CronExpression string    `db:"cron_expression"`
	Timezone       string    `db:"timezone"`
	CreatedAt      time.Time `db:"created_at"`
	UpdatedAt      time.Time `db:"updated_at"`
}

type ExecutionStatus string

const (
	EsPending   ExecutionStatus = "pending"
	EsRunning   ExecutionStatus = "running"
	EsCompleted ExecutionStatus = "completed"
	EsFailed    ExecutionStatus = "failed"
	EsCancelled ExecutionStatus = "cancelled"
	EsLapsed    ExecutionStatus = "lapsed"
)

type TasksExecution struct {
	ID        int64           `db:"id"`
	JobID     int64           `db:"job_id"`
	Status    ExecutionStatus `db:"status"`
	StartTime null.Time       `db:"start_time"`
	EndTime   null.Time       `db:"end_time"`
	ExitCode  int             `db:"exit_code"`
	Output    null.String     `db:"output"`
	Error     null.String     `db:"error"`
	Attempts  int             `db:"attempts"`
	WorkerId  null.String     `db:"worker_id"`
	CreatedAt time.Time       `db:"created_at"`
}
