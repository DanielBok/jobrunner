package api_test

import (
	"testing"

	"github.com/guregu/null/v6"
	"github.com/stretchr/testify/assert"
	"jobrunner/internal/api"
	"jobrunner/internal/models"
)

func TestCreateTaskDefinitionRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		request api.CreateTaskDefinitionRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid request",
			request: api.CreateTaskDefinitionRequest{
				Name:           "Test Task",
				Command:        "echo test",
				TimeoutSeconds: 3600,
				MaxRetries:     3,
				Schedules: []struct {
					CronExpression string `json:"cronExpression"`
					Timezone       string `json:"timezone"`
				}{
					{
						CronExpression: "0 * * * *",
						Timezone:       "UTC",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "empty name",
			request: api.CreateTaskDefinitionRequest{
				Name:           "",
				Command:        "echo test",
				TimeoutSeconds: 3600,
				MaxRetries:     3,
			},
			wantErr: true,
			errMsg:  "name is empty",
		},
		{
			name: "whitespace name",
			request: api.CreateTaskDefinitionRequest{
				Name:           "  ",
				Command:        "echo test",
				TimeoutSeconds: 3600,
				MaxRetries:     3,
			},
			wantErr: true,
			errMsg:  "name is empty",
		},
		{
			name: "empty command",
			request: api.CreateTaskDefinitionRequest{
				Name:           "Test Task",
				Command:        "",
				TimeoutSeconds: 3600,
				MaxRetries:     3,
			},
			wantErr: true,
			errMsg:  "command is empty",
		},
		{
			name: "negative timeout",
			request: api.CreateTaskDefinitionRequest{
				Name:           "Test Task",
				Command:        "echo test",
				TimeoutSeconds: -1,
				MaxRetries:     3,
			},
			wantErr: true,
			errMsg:  "timeoutSeconds must be >= 0",
		},
		{
			name: "negative max retries",
			request: api.CreateTaskDefinitionRequest{
				Name:           "Test Task",
				Command:        "echo test",
				TimeoutSeconds: 3600,
				MaxRetries:     -1,
			},
			wantErr: true,
			errMsg:  "maxRetries must be >= 0",
		},
		{
			name: "empty cron expression",
			request: api.CreateTaskDefinitionRequest{
				Name:           "Test Task",
				Command:        "echo test",
				TimeoutSeconds: 3600,
				MaxRetries:     3,
				Schedules: []struct {
					CronExpression string `json:"cronExpression"`
					Timezone       string `json:"timezone"`
				}{
					{
						CronExpression: "",
						Timezone:       "UTC",
					},
				},
			},
			wantErr: true,
			errMsg:  "schedule 1 has an empty cron expression",
		},
		{
			name: "empty timezone defaults to UTC",
			request: api.CreateTaskDefinitionRequest{
				Name:           "Test Task",
				Command:        "echo test",
				TimeoutSeconds: 3600,
				MaxRetries:     3,
				Schedules: []struct {
					CronExpression string `json:"cronExpression"`
					Timezone       string `json:"timezone"`
				}{
					{
						CronExpression: "0 * * * *",
						Timezone:       "",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "negative lookback window",
			request: api.CreateTaskDefinitionRequest{
				Name:           "Test Task",
				Command:        "echo test",
				TimeoutSeconds: 3600,
				MaxRetries:     3,
				Dependencies: []struct {
					DependsOn         int64                    `json:"dependsOn"`
					LookbackWindow    int64                    `json:"lookbackWindow"`
					MinWaitTime       int64                    `json:"minWaitTime"`
					RequiredCondition models.RequiredCondition `json:"requiredCondition"`
				}{
					{
						DependsOn:         1,
						LookbackWindow:    -1,
						MinWaitTime:       0,
						RequiredCondition: models.ReqCondSuccess,
					},
				},
			},
			wantErr: true,
			errMsg:  "dependency 1 lookbackWindow must be >= 0",
		},
		{
			name: "negative min wait time",
			request: api.CreateTaskDefinitionRequest{
				Name:           "Test Task",
				Command:        "echo test",
				TimeoutSeconds: 3600,
				MaxRetries:     3,
				Dependencies: []struct {
					DependsOn         int64                    `json:"dependsOn"`
					LookbackWindow    int64                    `json:"lookbackWindow"`
					MinWaitTime       int64                    `json:"minWaitTime"`
					RequiredCondition models.RequiredCondition `json:"requiredCondition"`
				}{
					{
						DependsOn:         1,
						LookbackWindow:    3600,
						MinWaitTime:       -1,
						RequiredCondition: models.ReqCondSuccess,
					},
				},
			},
			wantErr: true,
			errMsg:  "dependency 1 minWaitTime must be >= 0",
		},
		{
			name: "multiple validation errors",
			request: api.CreateTaskDefinitionRequest{
				Name:           "",
				Command:        "",
				TimeoutSeconds: -1,
				MaxRetries:     -1,
			},
			wantErr: true,
			errMsg:  "name is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUpdateTaskDefinitionRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		request api.UpdateTaskDefinitionRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid request",
			request: api.UpdateTaskDefinitionRequest{
				Name:           "Updated Task",
				Description:    null.StringFrom("Updated description"),
				ImageName:      null.StringFrom("alpine:latest"),
				Command:        "echo updated",
				TimeoutSeconds: 7200,
				MaxRetries:     5,
				IsActive:       true,
			},
			wantErr: false,
		},
		{
			name: "empty name",
			request: api.UpdateTaskDefinitionRequest{
				Name:           "",
				Command:        "echo updated",
				TimeoutSeconds: 7200,
				MaxRetries:     5,
			},
			wantErr: true,
			errMsg:  "name is empty",
		},
		{
			name: "empty command",
			request: api.UpdateTaskDefinitionRequest{
				Name:           "Updated Task",
				Command:        "",
				TimeoutSeconds: 7200,
				MaxRetries:     5,
			},
			wantErr: true,
			errMsg:  "command is empty",
		},
		{
			name: "zero timeout",
			request: api.UpdateTaskDefinitionRequest{
				Name:           "Updated Task",
				Command:        "echo updated",
				TimeoutSeconds: 0,
				MaxRetries:     5,
			},
			wantErr: true,
			errMsg:  "TimeoutSeconds must be > 0",
		},
		{
			name: "negative timeout",
			request: api.UpdateTaskDefinitionRequest{
				Name:           "Updated Task",
				Command:        "echo updated",
				TimeoutSeconds: -1,
				MaxRetries:     5,
			},
			wantErr: true,
			errMsg:  "TimeoutSeconds must be > 0",
		},
		{
			name: "negative max retries",
			request: api.UpdateTaskDefinitionRequest{
				Name:           "Updated Task",
				Command:        "echo updated",
				TimeoutSeconds: 7200,
				MaxRetries:     -1,
			},
			wantErr: true,
			errMsg:  "maxRetries must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCreateTaskDependencyRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		request api.CreateTaskDependencyRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid request",
			request: api.CreateTaskDependencyRequest{
				DependsOn:         1,
				LookbackWindow:    3600,
				MinWaitTime:       300,
				RequiredCondition: models.ReqCondSuccess,
			},
			wantErr: false,
		},
		{
			name: "zero lookback window",
			request: api.CreateTaskDependencyRequest{
				DependsOn:         1,
				LookbackWindow:    0,
				MinWaitTime:       300,
				RequiredCondition: models.ReqCondSuccess,
			},
			wantErr: true,
			errMsg:  "lookbackWindow must be > 0",
		},
		{
			name: "negative lookback window",
			request: api.CreateTaskDependencyRequest{
				DependsOn:         1,
				LookbackWindow:    -1,
				MinWaitTime:       300,
				RequiredCondition: models.ReqCondSuccess,
			},
			wantErr: true,
			errMsg:  "lookbackWindow must be > 0",
		},
		{
			name: "negative min wait time",
			request: api.CreateTaskDependencyRequest{
				DependsOn:         1,
				LookbackWindow:    3600,
				MinWaitTime:       -1,
				RequiredCondition: models.ReqCondSuccess,
			},
			wantErr: true,
			errMsg:  "minWaitTime must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUpdateTaskDependencyRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		request api.UpdateTaskDependencyRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid request",
			request: api.UpdateTaskDependencyRequest{
				LookbackWindow:    7200,
				MinWaitTime:       600,
				RequiredCondition: models.ReqCondFailure,
			},
			wantErr: false,
		},
		{
			name: "zero lookback window",
			request: api.UpdateTaskDependencyRequest{
				LookbackWindow:    0,
				MinWaitTime:       600,
				RequiredCondition: models.ReqCondFailure,
			},
			wantErr: true,
			errMsg:  "lookbackWindow must be > 0",
		},
		{
			name: "negative lookback window",
			request: api.UpdateTaskDependencyRequest{
				LookbackWindow:    -1,
				MinWaitTime:       600,
				RequiredCondition: models.ReqCondFailure,
			},
			wantErr: true,
			errMsg:  "lookbackWindow must be > 0",
		},
		{
			name: "negative min wait time",
			request: api.UpdateTaskDependencyRequest{
				LookbackWindow:    7200,
				MinWaitTime:       -1,
				RequiredCondition: models.ReqCondFailure,
			},
			wantErr: true,
			errMsg:  "minWaitTime must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCreateTaskScheduleRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		request api.CreateTaskScheduleRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid request",
			request: api.CreateTaskScheduleRequest{
				CronExpression: "*/15 * * * *",
				Timezone:       "Europe/London",
			},
			wantErr: false,
		},
		{
			name: "empty cron expression",
			request: api.CreateTaskScheduleRequest{
				CronExpression: "",
				Timezone:       "UTC",
			},
			wantErr: true,
			errMsg:  "cronExpression must not be empty",
		},
		{
			name: "whitespace cron expression",
			request: api.CreateTaskScheduleRequest{
				CronExpression: "  ",
				Timezone:       "UTC",
			},
			wantErr: true,
			errMsg:  "cronExpression must not be empty",
		},
		{
			name: "empty timezone defaults to UTC",
			request: api.CreateTaskScheduleRequest{
				CronExpression: "0 * * * *",
				Timezone:       "",
			},
			wantErr: false,
		},
		{
			name: "whitespace timezone defaults to UTC",
			request: api.CreateTaskScheduleRequest{
				CronExpression: "0 * * * *",
				Timezone:       "  ",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
				if tt.request.Timezone == "" || tt.request.Timezone == "  " {
					assert.Equal(t, "UTC", tt.request.Timezone)
				}
			}
		})
	}
}

func TestUpdateTaskScheduleRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		request api.UpdateTaskScheduleRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid request",
			request: api.UpdateTaskScheduleRequest{
				CronExpression: "0 */2 * * *",
				Timezone:       "Asia/Tokyo",
			},
			wantErr: false,
		},
		{
			name: "empty cron expression",
			request: api.UpdateTaskScheduleRequest{
				CronExpression: "",
				Timezone:       "UTC",
			},
			wantErr: true,
			errMsg:  "cronExpression must not be empty",
		},
		{
			name: "whitespace cron expression",
			request: api.UpdateTaskScheduleRequest{
				CronExpression: "  ",
				Timezone:       "UTC",
			},
			wantErr: true,
			errMsg:  "cronExpression must not be empty",
		},
		{
			name: "empty timezone defaults to UTC",
			request: api.UpdateTaskScheduleRequest{
				CronExpression: "0 * * * *",
				Timezone:       "",
			},
			wantErr: false,
		},
		{
			name: "whitespace timezone defaults to UTC",
			request: api.UpdateTaskScheduleRequest{
				CronExpression: "0 * * * *",
				Timezone:       "  ",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
				if tt.request.Timezone == "" || tt.request.Timezone == "  " {
					assert.Equal(t, "UTC", tt.request.Timezone)
				}
			}
		})
	}
}
