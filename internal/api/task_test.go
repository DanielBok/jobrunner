package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"jobrunner/internal/api"
	"jobrunner/internal/config"
	"jobrunner/internal/database"
	"jobrunner/internal/models"
)

func TestTaskRouter_GetDefinitions(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Clean up test data
	cleanupDatabase(t, db)

	// Insert test task definitions
	task1ID := insertTestTaskDefinition(t, db, "Test Task 1", "echo Test Task 1")
	task2ID := insertTestTaskDefinition(t, db, "Test Task 2", "echo Test Task 2")

	// Insert schedule for task 1
	insertTestSchedule(t, db, task1ID, "0 * * * *", "UTC")

	// Insert dependency for task 2 (depends on task 1)
	insertTestDependency(t, db, task2ID, task1ID, "success", 3600, 0)

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create a request to the endpoint
	req, err := http.NewRequest("GET", "/definitions", nil)
	require.NoError(t, err)

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.GetDefinitions(rr, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse the response
	var definitions []models.TaskDefinition
	err = json.Unmarshal(rr.Body.Bytes(), &definitions)
	require.NoError(t, err)

	// Verify the results
	assert.Len(t, definitions, 2)

	// Find our tasks by ID
	var foundTask1, foundTask2 bool
	for _, def := range definitions {
		switch def.ID {
		case task1ID:
			assert.Equal(t, "Test Task 1", def.Name)
			foundTask1 = true
		case task2ID:
			assert.Equal(t, "Test Task 2", def.Name)
			foundTask2 = true
		}
	}

	assert.True(t, foundTask1, "Task 1 not found in response")
	assert.True(t, foundTask2, "Task 2 not found in response")
}

func TestTaskRouter_AddDefinition(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Clean up test data
	cleanupDatabase(t, db)

	// Create a dependency task
	dependencyTaskID := insertTestTaskDefinition(t, db, "Dependency Task", "echo Dependency")

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create the task definition request
	newTask := api.CreateTaskDefinition{
		Name:           "New Test Task",
		Command:        "echo New Task",
		TimeoutSeconds: 3600,
		MaxRetries:     2,
		Schedules: []struct {
			CronExpression string `json:"cron_expression"`
			Timezone       string `json:"timezone"`
		}{
			{
				CronExpression: "0 0 * * *",
				Timezone:       "UTC",
			},
			{
				CronExpression: "0 12 * * *",
				Timezone:       "UTC",
			},
		},
		Dependencies: []struct {
			DependsOn         int64                    `json:"depends_on"`
			LookbackWindow    int64                    `json:"lookback_window"`
			MinWaitTime       int64                    `json:"min_wait_time"`
			RequiredCondition models.RequiredCondition `json:"required_condition"`
		}{
			{
				DependsOn:         dependencyTaskID,
				LookbackWindow:    3600,
				MinWaitTime:       0,
				RequiredCondition: "success",
			},
		},
	}

	// Convert task to JSON
	taskJSON, err := json.Marshal(newTask)
	require.NoError(t, err)

	// Create a request to the endpoint
	req, err := http.NewRequest("POST", "/definitions", bytes.NewBuffer(taskJSON))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.AddDefinition(rr, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse the response
	var response api.ListTaskDefinition
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)

	// Verify the response
	assert.Equal(t, "New Test Task", response.Name)
	assert.Equal(t, "echo New Task", response.Command)
	assert.Equal(t, int64(3600), response.TimeoutSeconds)
	assert.Equal(t, 2, response.MaxRetries)
	assert.True(t, response.IsActive)

	// Verify schedules
	require.Len(t, response.Schedules, 2)
	assert.Equal(t, "0 0 * * *", response.Schedules[0].CronExpression)
	assert.Equal(t, "0 12 * * *", response.Schedules[1].CronExpression)

	// Verify dependencies
	require.Len(t, response.Dependencies, 1)
	assert.Equal(t, dependencyTaskID, response.Dependencies[0].DependsOn)
	assert.Equal(t, int64(3600), response.Dependencies[0].LookbackWindow)
	assert.Equal(t, models.ReqCondSuccess, response.Dependencies[0].RequiredCondition)

	// Verify task was actually created in database
	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM task.definition WHERE id = $1", response.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "Task definition not found in database")

	// Verify schedules were created
	err = db.Get(&count, "SELECT COUNT(*) FROM task.schedule WHERE task_id = $1", response.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "Task schedules not found in database")

	// Verify dependencies were created
	err = db.Get(&count, "SELECT COUNT(*) FROM task.dependency WHERE task_id = $1", response.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "Task dependencies not found in database")
}

func TestTaskRouter_AddDefinition_ValidationFailure(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create an invalid task definition request (missing required name)
	invalidTask := struct {
		Command string `json:"command"`
	}{
		Command: "echo Invalid Task",
	}

	// Convert task to JSON
	taskJSON, err := json.Marshal(invalidTask)
	require.NoError(t, err)

	// Create a request to the endpoint
	req, err := http.NewRequest("POST", "/definitions", bytes.NewBuffer(taskJSON))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.AddDefinition(rr, req)

	// Check the status code - should be a bad request
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

// Helper functions for test setup
func cleanupDatabase(t *testing.T, db *sqlx.DB) {
	_, err := db.Exec("TRUNCATE TABLE task.run, task.dependency, task.schedule, task.definition CASCADE")
	require.NoError(t, err)
}

func insertTestTaskDefinition(t *testing.T, db *sqlx.DB, name, command string) int64 {
	var id int64
	err := db.QueryRow(`
		INSERT INTO task.definition (name, command, timeout_seconds, max_retries, is_active)
		VALUES ($1, $2, 3600, 3, TRUE)
		RETURNING id
	`, name, command).Scan(&id)

	require.NoError(t, err)
	return id
}

func insertTestSchedule(t *testing.T, db *sqlx.DB, taskID int64, cronExpression, timezone string) int64 {
	var id int64
	err := db.QueryRow(`
		INSERT INTO task.schedule (task_id, cron_expression, timezone)
		VALUES ($1, $2, $3)
		RETURNING id
	`, taskID, cronExpression, timezone).Scan(&id)

	require.NoError(t, err)
	return id
}

func insertTestDependency(t *testing.T, db *sqlx.DB, taskID, dependsOn int64, condition models.RequiredCondition, lookbackWindow, minWaitTime int) int64 {
	var id int64
	err := db.QueryRow(`
		INSERT INTO task.dependency (task_id, depends_on, required_condition, lookback_window, min_wait_time)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, taskID, dependsOn, condition, lookbackWindow, minWaitTime).Scan(&id)

	require.NoError(t, err)
	return id
}
