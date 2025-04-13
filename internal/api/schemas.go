package api

import (
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
	TimeoutSeconds int64       `json:"timeoutSeconds"`
	MaxRetries     int         `json:"maxRetries"`

	Schedules []struct {
		CronExpression string `json:"cron_expression"`
		Timezone       string `json:"timezone"`
	} `json:"schedules"`

	Dependencies []struct {
		DependsOn         int64                    `json:"dependsOn"`
		LookbackWindow    int64                    `json:"lookbackWindow"`
		MinWaitTime       int64                    `json:"minWaitTime"`
		RequiredCondition models.RequiredCondition `json:"requiredCondition"`
	} `json:"dependencies"`
}
