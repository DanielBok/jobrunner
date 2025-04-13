package api

import (
	"errors"
	"fmt"
	"strings"

	"github.com/guregu/null/v6"
	"jobrunner/internal/models"
)

type ListTaskDefinition struct {
	models.TaskDefinition
	Schedules    []*models.TaskSchedule   `json:"schedules"`
	Dependencies []*models.TaskDependency `json:"dependencies"`
}

type CreateTaskDefinition struct {
	Name           string      `json:"name"`
	Description    null.String `json:"description"`
	ImageName      null.String `json:"imageName"`
	Command        string      `json:"command"`
	TimeoutSeconds int64       `json:"timeout_seconds"`
	MaxRetries     int         `json:"max_retries"`

	Schedules []struct {
		CronExpression string `json:"cron_expression"`
		Timezone       string `json:"timezone"`
	} `json:"schedules"`

	Dependencies []struct {
		DependsOn         int64                    `json:"depends_on"`
		LookbackWindow    int64                    `json:"lookback_window"`
		MinWaitTime       int64                    `json:"min_wait_time"`
		RequiredCondition models.RequiredCondition `json:"required_condition"`
	} `json:"dependencies"`
}

func (c *CreateTaskDefinition) validate() error {
	var errs []error

	c.Name = strings.TrimSpace(c.Name)
	if c.Name == "" {
		errs = append(errs, errors.New("name is empty"))
	}

	c.Command = strings.TrimSpace(c.Command)
	if c.Command == "" {
		errs = append(errs, errors.New("command is empty"))
	}

	if c.MaxRetries < 0 {
		errs = append(errs, errors.New("max_retries must be >= 0"))
	}

	for i, s := range c.Schedules {
		s.Timezone = strings.TrimSpace(s.Timezone)
		if s.Timezone == "" {
			s.Timezone = "UTC"
		}

		s.CronExpression = strings.TrimSpace(s.CronExpression)
		if s.CronExpression == "" {
			errs = append(errs, fmt.Errorf("schedule %d has an empty cron expression", i+1))
		}
	}

	for i, d := range c.Dependencies {
		if d.LookbackWindow < 0 {
			errs = append(errs, fmt.Errorf("dependency %d lookback_window must be >= 0", i+1))
		}

		if d.MinWaitTime < 0 {
			errs = append(errs, fmt.Errorf("dependency %d min_wait_time must be >= 0", i+1))
		}
	}

	return errors.Join(errs...)
}
