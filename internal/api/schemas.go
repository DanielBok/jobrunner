package api

import (
	"errors"
	"fmt"
	"strings"

	"github.com/guregu/null/v6"
	"jobrunner/internal/models"
)

// TaskDefinitionResponse defines the schema for the task definition response
type TaskDefinitionResponse struct {
	models.TaskDefinition
	Schedules    []*models.TaskSchedule   `json:"schedules"`
	Dependencies []*models.TaskDependency `json:"dependencies"`
}

// CreateTaskDefinitionRequest defines the schema to create a new task definition
type CreateTaskDefinitionRequest struct {
	Name           string      `json:"name"`
	Description    null.String `json:"description"`
	ImageName      null.String `json:"imageName"`
	Command        string      `json:"command"`
	TimeoutSeconds int64       `json:"timeoutSeconds"`
	MaxRetries     int         `json:"maxRetries"`

	Schedules []struct {
		CronExpression string `json:"cronExpression"`
		Timezone       string `json:"timezone"`
	} `json:"schedules"`

	Dependencies []struct {
		DependsOn         int64                    `json:"dependsOn"`
		LookbackWindow    int64                    `json:"lookbackWindow"`
		MinWaitTime       int64                    `json:"minWaitTime"`
		RequiredCondition models.RequiredCondition `json:"requiredCondition"`
	} `json:"dependencies"`
}

func (r *CreateTaskDefinitionRequest) Validate() error {
	var errs []error

	r.Name = strings.TrimSpace(r.Name)
	if r.Name == "" {
		errs = append(errs, errors.New("name is empty"))
	}

	r.Command = strings.TrimSpace(r.Command)
	if r.Command == "" {
		errs = append(errs, errors.New("command is empty"))
	}

	if r.TimeoutSeconds < 0 {
		errs = append(errs, errors.New("timeoutSeconds must be >= 0"))
	}

	if r.MaxRetries < 0 {
		errs = append(errs, errors.New("maxRetries must be >= 0"))
	}

	for i, s := range r.Schedules {
		s.Timezone = strings.TrimSpace(s.Timezone)
		if s.Timezone == "" {
			s.Timezone = "UTC"
		}

		s.CronExpression = strings.TrimSpace(s.CronExpression)
		if s.CronExpression == "" {
			errs = append(errs, fmt.Errorf("schedule %d has an empty cron expression", i+1))
		}
	}

	for i, d := range r.Dependencies {
		if d.LookbackWindow < 0 {
			errs = append(errs, fmt.Errorf("dependency %d lookbackWindow must be >= 0", i+1))
		}

		if d.MinWaitTime < 0 {
			errs = append(errs, fmt.Errorf("dependency %d minWaitTime must be >= 0", i+1))
		}
	}

	return errors.Join(errs...)
}

// UpdateTaskDefinitionRequest defines the schema for updating a task definition
type UpdateTaskDefinitionRequest struct {
	Name           string      `json:"name"`
	Description    null.String `json:"description"`
	ImageName      null.String `json:"imageName"`
	Command        string      `json:"command"`
	TimeoutSeconds int64       `json:"timeoutSeconds"`
	MaxRetries     int         `json:"maxRetries"`
	IsActive       bool        `json:"isActive"`
}

func (r *UpdateTaskDefinitionRequest) Validate() error {
	var errs []error

	r.Name = strings.TrimSpace(r.Name)
	if r.Name == "" {
		errs = append(errs, errors.New("name is empty"))
	}

	r.Command = strings.TrimSpace(r.Command)
	if r.Command == "" {
		errs = append(errs, errors.New("command is empty"))
	}

	if r.TimeoutSeconds <= 0 {
		errs = append(errs, errors.New("TimeoutSeconds must be > 0"))
	}

	if r.MaxRetries < 0 {
		errs = append(errs, errors.New("maxRetries must be >= 0"))
	}
	return errors.Join(errs...)
}

// DeleteTaskDefinitionResponse defines the schema for the task definition delete response
type DeleteTaskDefinitionResponse struct {
	TaskID  int64  `json:"taskId"`
	Message string `json:"message"`
}

// UpdateTaskDependencyRequest defines the schema for updating a task dependency
type UpdateTaskDependencyRequest struct {
	LookbackWindow    int64                    `json:"lookbackWindow"`
	MinWaitTime       int64                    `json:"minWaitTime"`
	RequiredCondition models.RequiredCondition `json:"requiredCondition"`
}

func (r *UpdateTaskDependencyRequest) Validate() error {
	var errs []error

	if r.LookbackWindow <= 0 {
		errs = append(errs, errors.New("lookbackWindow must be > 0"))
	}

	if r.MinWaitTime < 0 {
		errs = append(errs, errors.New("minWaitTime must be >= 0"))
	}

	return errors.Join(errs...)
}

// CreateTaskDependencyRequest defines the schema for creating a new task dependency
type CreateTaskDependencyRequest struct {
	DependsOn         int64                    `json:"dependsOn"`
	LookbackWindow    int64                    `json:"lookbackWindow"`
	MinWaitTime       int64                    `json:"minWaitTime"`
	RequiredCondition models.RequiredCondition `json:"requiredCondition"`
}

func (r *CreateTaskDependencyRequest) Validate() error {
	var errs []error

	if r.LookbackWindow <= 0 {
		errs = append(errs, errors.New("lookbackWindow must be > 0"))
	}

	if r.MinWaitTime < 0 {
		errs = append(errs, errors.New("minWaitTime must be >= 0"))
	}

	return errors.Join(errs...)
}

// DeleteTaskDependencyResponse defines the schema for the task dependency delete response
type DeleteTaskDependencyResponse struct {
	TaskID       int64  `json:"taskId"`
	DependencyId int64  `json:"dependencyId"`
	Message      string `json:"message"`
}

// UpdateTaskScheduleRequest defines the schema for updating a task schedule
type UpdateTaskScheduleRequest struct {
	CronExpression string `json:"cronExpression"`
	Timezone       string `json:"timezone"`
}

func (r *UpdateTaskScheduleRequest) Validate() error {
	var errs []error

	r.CronExpression = strings.TrimSpace(r.CronExpression)
	if r.CronExpression == "" {
		errs = append(errs, errors.New("cronExpression must not be empty"))
	}

	r.Timezone = strings.TrimSpace(r.Timezone)
	if r.Timezone == "" {
		r.Timezone = "UTC"
	}

	return errors.Join(errs...)
}

// CreateTaskScheduleRequest defines the schema for creating a new task schedule
type CreateTaskScheduleRequest struct {
	CronExpression string `json:"cronExpression"`
	Timezone       string `json:"timezone"`
}

func (r *CreateTaskScheduleRequest) Validate() error {
	var errs []error

	r.CronExpression = strings.TrimSpace(r.CronExpression)
	if r.CronExpression == "" {
		errs = append(errs, errors.New("cronExpression must not be empty"))
	}

	r.Timezone = strings.TrimSpace(r.Timezone)
	if r.Timezone == "" {
		r.Timezone = "UTC"
	}

	return errors.Join(errs...)
}

// DeleteTaskScheduleResponse defines the schema for the task schedule delete response
type DeleteTaskScheduleResponse struct {
	TaskID     int64  `json:"taskId"`
	ScheduleID int64  `json:"scheduleId"`
	Message    string `json:"message"`
}
