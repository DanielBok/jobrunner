package models

import (
	"time"

	"github.com/guregu/null/v6"
)

// This file contains all the models under the `tasks` schema

// TaskDefinition is a models representing the `tasks.definition` table
type TaskDefinition struct {
	ID             int64       `db:"id" json:"id"`
	Name           string      `db:"name" json:"name"`
	Description    null.String `db:"description" json:"description"`
	ImageName      null.String `db:"image_name" json:"imageName"`
	Command        string      `db:"command" json:"command"`
	TimeoutSeconds int64       `db:"timeout_seconds" json:"timeoutSeconds"`
	MaxRetries     int         `db:"max_retries" json:"maxRetries"`
	CreatedAt      time.Time   `db:"created_at" json:"createdAt"`
	UpdatedAt      time.Time   `db:"updated_at" json:"updatedAt"`
	IsActive       bool        `db:"is_active" json:"isActive"`
}

type RequiredCondition string

const (
	ReqCondSuccess    RequiredCondition = "success"
	ReqCondCompletion RequiredCondition = "completion"
	ReqCondFailure    RequiredCondition = "failure"
	ReqCondCancelled  RequiredCondition = "cancelled"
	ReqCondLapsed     RequiredCondition = "lapsed"
)

// TaskDependency is a models representing the `tasks.dependency` table
type TaskDependency struct {
	ID                int64             `db:"id" json:"id"`
	TaskID            int64             `db:"task_id" json:"taskID"`
	DependsOn         int64             `db:"depends_on" json:"dependsOn"`
	LookbackWindow    int64             `db:"lookback_window" json:"lookbackWindow"`
	MinWaitTime       int64             `db:"min_wait_time" json:"minWaitTime"`
	RequiredCondition RequiredCondition `db:"required_condition" json:"requiredCondition"`
	CreatedAt         time.Time         `db:"created_at" json:"createdAt"`
	UpdatedAt         time.Time         `db:"updated_at" json:"updatedAt"`
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
	RunStatusPending   RunStatus = "pending"
	RunStatusRunning   RunStatus = "running"
	RunStatusCompleted RunStatus = "completed"
	RunStatusFailed    RunStatus = "failed"
	RunStatusCancelled RunStatus = "cancelled"
	RunStatusLapsed    RunStatus = "lapsed"
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
