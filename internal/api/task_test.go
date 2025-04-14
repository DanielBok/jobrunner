package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/guregu/null/v6"
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
	newTask := api.CreateTaskDefinitionRequest{
		Name:           "New Test Task",
		Command:        "echo New Task",
		TimeoutSeconds: 3600,
		MaxRetries:     2,
		Schedules: []struct {
			CronExpression string `json:"cronExpression"`
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
			DependsOn         int64                    `json:"dependsOn"`
			LookbackWindow    int64                    `json:"lookbackWindow"`
			MinWaitTime       int64                    `json:"minWaitTime"`
			RequiredCondition models.RequiredCondition `json:"requiredCondition"`
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
	var response api.TaskDefinitionResponse
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

func TestTaskRouter_UpdateDefinition(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Clean up test data
	cleanupDatabase(t, db)

	// Insert test task definition
	taskID := insertTestTaskDefinition(t, db, "Original Task", "echo Original")

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create the task update request
	updateTask := api.UpdateTaskDefinitionRequest{
		Name:           "Updated Task",
		Description:    null.StringFrom("Updated description"),
		ImageName:      null.StringFrom("alpine:latest"),
		Command:        "echo Updated",
		TimeoutSeconds: 7200,
		MaxRetries:     5,
		IsActive:       true,
	}

	// Convert task to JSON
	taskJSON, err := json.Marshal(updateTask)
	require.NoError(t, err)

	// Create a request to the endpoint
	req, err := http.NewRequest("PUT", "/definitions/"+fmt.Sprint(taskID), bytes.NewBuffer(taskJSON))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	// Add task ID to route parameters
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", fmt.Sprint(taskID))
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.UpdateDefinition(rr, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse the response
	var response api.TaskDefinitionResponse
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)

	// Verify the response
	assert.Equal(t, "Updated Task", response.Name)
	assert.Equal(t, "Updated description", response.Description.String)
	assert.Equal(t, "alpine:latest", response.ImageName.String)
	assert.Equal(t, "echo Updated", response.Command)
	assert.Equal(t, int64(7200), response.TimeoutSeconds)
	assert.Equal(t, 5, response.MaxRetries)
	assert.True(t, response.IsActive)

	// Verify task was actually updated in database
	var updatedTask models.TaskDefinition
	err = db.Get(&updatedTask, "SELECT * FROM task.definition WHERE id = $1", taskID)
	require.NoError(t, err)
	assert.Equal(t, "Updated Task", updatedTask.Name)
	assert.Equal(t, "echo Updated", updatedTask.Command)
}

func TestTaskRouter_DeleteDefinition(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Clean up test data
	cleanupDatabase(t, db)

	// Insert test task definition
	taskID := insertTestTaskDefinition(t, db, "Task To Delete", "echo Delete Me")

	// Insert a schedule for the task
	insertTestSchedule(t, db, taskID, "0 * * * *", "UTC")

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create a request to the endpoint
	req, err := http.NewRequest("DELETE", "/definitions/"+fmt.Sprint(taskID), nil)
	require.NoError(t, err)

	// Add task ID to route parameters
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", fmt.Sprint(taskID))
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.DeleteDefinition(rr, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse the response
	var response api.DeleteTaskDefinitionResponse
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)

	// Verify the response
	assert.Equal(t, taskID, response.TaskID)
	assert.Contains(t, response.Message, fmt.Sprintf("%d", taskID))

	// Verify task was actually deleted from database
	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM task.definition WHERE id = $1", taskID)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "Task definition should be deleted")

	// Verify schedules were deleted (cascaded)
	err = db.Get(&count, "SELECT COUNT(*) FROM task.schedule WHERE task_id = $1", taskID)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "Task schedules should be deleted")
}

func TestTaskRouter_GetDependencies(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Clean up test data
	cleanupDatabase(t, db)

	// Insert test tasks
	taskID := insertTestTaskDefinition(t, db, "Main Task", "echo Main")
	dependency1ID := insertTestTaskDefinition(t, db, "Dependency 1", "echo Dep1")
	dependency2ID := insertTestTaskDefinition(t, db, "Dependency 2", "echo Dep2")

	// Insert dependencies
	depID1 := insertTestDependency(t, db, taskID, dependency1ID, "success", 3600, 0)
	depID2 := insertTestDependency(t, db, taskID, dependency2ID, "completion", 7200, 300)

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create a request to the endpoint
	req, err := http.NewRequest("GET", "/definitions/"+fmt.Sprint(taskID)+"/dependencies", nil)
	require.NoError(t, err)

	// Add task ID to route parameters
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", fmt.Sprint(taskID))
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.GetDependencies(rr, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse the response
	var dependencies []*models.TaskDependency
	err = json.Unmarshal(rr.Body.Bytes(), &dependencies)
	require.NoError(t, err)

	// Verify the response
	assert.Len(t, dependencies, 2)

	// Find dependencies by ID
	var foundDep1, foundDep2 bool
	for _, dep := range dependencies {
		if dep.ID == depID1 {
			assert.Equal(t, dependency1ID, dep.DependsOn)
			assert.Equal(t, models.ReqCondSuccess, dep.RequiredCondition)
			assert.Equal(t, int64(3600), dep.LookbackWindow)
			foundDep1 = true
		} else if dep.ID == depID2 {
			assert.Equal(t, dependency2ID, dep.DependsOn)
			assert.Equal(t, models.ReqCondCompletion, dep.RequiredCondition)
			assert.Equal(t, int64(7200), dep.LookbackWindow)
			assert.Equal(t, int64(300), dep.MinWaitTime)
			foundDep2 = true
		}
	}

	assert.True(t, foundDep1, "Dependency 1 not found in response")
	assert.True(t, foundDep2, "Dependency 2 not found in response")
}

func TestTaskRouter_AddDependency(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Clean up test data
	cleanupDatabase(t, db)

	// Insert test tasks
	taskID := insertTestTaskDefinition(t, db, "Main Task", "echo Main")
	dependencyTaskID := insertTestTaskDefinition(t, db, "Dependency Task", "echo Dep")

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create the dependency request
	newDependency := api.CreateTaskDependencyRequest{
		DependsOn:         dependencyTaskID,
		LookbackWindow:    7200,
		MinWaitTime:       600,
		RequiredCondition: models.ReqCondFailure,
	}

	// Convert request to JSON
	depJSON, err := json.Marshal(newDependency)
	require.NoError(t, err)

	// Create a request to the endpoint
	req, err := http.NewRequest("POST", "/definitions/"+fmt.Sprint(taskID)+"/dependencies", bytes.NewBuffer(depJSON))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	// Add task ID to route parameters
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", fmt.Sprint(taskID))
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.AddDependency(rr, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse the response
	var response models.TaskDependency
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)

	// Verify the response
	assert.Equal(t, taskID, response.TaskID)
	assert.Equal(t, dependencyTaskID, response.DependsOn)
	assert.Equal(t, int64(7200), response.LookbackWindow)
	assert.Equal(t, int64(600), response.MinWaitTime)
	assert.Equal(t, models.ReqCondFailure, response.RequiredCondition)

	// Verify dependency was actually created in database
	var dbDependency models.TaskDependency
	err = db.Get(&dbDependency, "SELECT * FROM task.dependency WHERE id = $1", response.ID)
	require.NoError(t, err)
	assert.Equal(t, taskID, dbDependency.TaskID)
	assert.Equal(t, dependencyTaskID, dbDependency.DependsOn)
}

func TestTaskRouter_UpdateDependency(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Clean up test data
	cleanupDatabase(t, db)

	// Insert test tasks
	taskID := insertTestTaskDefinition(t, db, "Main Task", "echo Main")
	dependencyTaskID := insertTestTaskDefinition(t, db, "Dependency Task", "echo Dep")

	// Insert dependency
	depID := insertTestDependency(t, db, taskID, dependencyTaskID, "success", 3600, 0)

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create the dependency update request
	updateDependency := api.UpdateTaskDependencyRequest{
		LookbackWindow:    10800,
		MinWaitTime:       900,
		RequiredCondition: models.ReqCondCancelled,
	}

	// Convert request to JSON
	depJSON, err := json.Marshal(updateDependency)
	require.NoError(t, err)

	// Create a request to the endpoint
	endpoint := fmt.Sprintf("/definitions/%d/dependencies/%d", taskID, depID)
	req, err := http.NewRequest("PUT", endpoint, bytes.NewBuffer(depJSON))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	// Add IDs to route parameters
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", fmt.Sprint(taskID))
	rctx.URLParams.Add("depId", fmt.Sprint(depID))
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.UpdateDependency(rr, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse the response
	var response models.TaskDependency
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)

	// Verify the response
	assert.Equal(t, depID, response.ID)
	assert.Equal(t, taskID, response.TaskID)
	assert.Equal(t, dependencyTaskID, response.DependsOn)
	assert.Equal(t, int64(10800), response.LookbackWindow)
	assert.Equal(t, int64(900), response.MinWaitTime)
	assert.Equal(t, models.ReqCondCancelled, response.RequiredCondition)

	// Verify dependency was actually updated in database
	var dbDependency models.TaskDependency
	err = db.Get(&dbDependency, "SELECT * FROM task.dependency WHERE id = $1", depID)
	require.NoError(t, err)
	assert.Equal(t, int64(10800), dbDependency.LookbackWindow)
	assert.Equal(t, int64(900), dbDependency.MinWaitTime)
	assert.Equal(t, models.ReqCondCancelled, dbDependency.RequiredCondition)
}

func TestTaskRouter_DeleteDependency(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Clean up test data
	cleanupDatabase(t, db)

	// Insert test tasks
	taskID := insertTestTaskDefinition(t, db, "Main Task", "echo Main")
	dependencyTaskID := insertTestTaskDefinition(t, db, "Dependency Task", "echo Dep")

	// Insert dependency
	depID := insertTestDependency(t, db, taskID, dependencyTaskID, "success", 3600, 0)

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create a request to the endpoint
	endpoint := fmt.Sprintf("/definitions/%d/dependencies/%d", taskID, depID)
	req, err := http.NewRequest("DELETE", endpoint, nil)
	require.NoError(t, err)

	// Add IDs to route parameters
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", fmt.Sprint(taskID))
	rctx.URLParams.Add("depId", fmt.Sprint(depID))
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.DeleteDependency(rr, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse the response
	var response api.DeleteTaskDependencyResponse
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)

	// Verify the response
	assert.Equal(t, taskID, response.TaskID)
	assert.Equal(t, depID, response.DependencyId)
	assert.Contains(t, response.Message, fmt.Sprintf("%d", depID))

	// Verify dependency was actually deleted from database
	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM task.dependency WHERE id = $1", depID)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "Dependency should be deleted")
}

func TestTaskRouter_GetSchedules(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Clean up test data
	cleanupDatabase(t, db)

	// Insert test task
	taskID := insertTestTaskDefinition(t, db, "Task With Schedules", "echo Scheduled")

	// Insert schedules
	scheduleID1 := insertTestSchedule(t, db, taskID, "0 0 * * *", "UTC")
	scheduleID2 := insertTestSchedule(t, db, taskID, "0 12 * * *", "America/New_York")

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create a request to the endpoint
	req, err := http.NewRequest("GET", "/definitions/"+fmt.Sprint(taskID)+"/schedules", nil)
	require.NoError(t, err)

	// Add task ID to route parameters
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", fmt.Sprint(taskID))
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.GetSchedules(rr, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse the response
	var schedules []*models.TaskSchedule
	err = json.Unmarshal(rr.Body.Bytes(), &schedules)
	require.NoError(t, err)

	// Verify the response
	assert.Len(t, schedules, 2)

	// Find schedules by ID
	var foundSch1, foundSch2 bool
	for _, sch := range schedules {
		if sch.ID == scheduleID1 {
			assert.Equal(t, taskID, sch.TaskID)
			assert.Equal(t, "0 0 * * *", sch.CronExpression)
			assert.Equal(t, "UTC", sch.Timezone)
			foundSch1 = true
		} else if sch.ID == scheduleID2 {
			assert.Equal(t, taskID, sch.TaskID)
			assert.Equal(t, "0 12 * * *", sch.CronExpression)
			assert.Equal(t, "America/New_York", sch.Timezone)
			foundSch2 = true
		}
	}

	assert.True(t, foundSch1, "Schedule 1 not found in response")
	assert.True(t, foundSch2, "Schedule 2 not found in response")
}

func TestTaskRouter_AddSchedule(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Clean up test data
	cleanupDatabase(t, db)

	// Insert test task
	taskID := insertTestTaskDefinition(t, db, "Task For Schedule", "echo Schedule")

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create the schedule request
	newSchedule := api.CreateTaskScheduleRequest{
		CronExpression: "*/15 * * * *",
		Timezone:       "Europe/London",
	}

	// Convert request to JSON
	scheduleJSON, err := json.Marshal(newSchedule)
	require.NoError(t, err)

	// Create a request to the endpoint
	req, err := http.NewRequest("POST", "/definitions/"+fmt.Sprint(taskID)+"/schedules", bytes.NewBuffer(scheduleJSON))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	// Add task ID to route parameters
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", fmt.Sprint(taskID))
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.AddSchedule(rr, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse the response
	var response models.TaskSchedule
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)

	// Verify the response
	assert.Equal(t, taskID, response.TaskID)
	assert.Equal(t, "*/15 * * * *", response.CronExpression)
	assert.Equal(t, "Europe/London", response.Timezone)

	// Verify schedule was actually created in database
	var dbSchedule models.TaskSchedule
	err = db.Get(&dbSchedule, "SELECT * FROM task.schedule WHERE id = $1", response.ID)
	require.NoError(t, err)
	assert.Equal(t, taskID, dbSchedule.TaskID)
	assert.Equal(t, "*/15 * * * *", dbSchedule.CronExpression)
	assert.Equal(t, "Europe/London", dbSchedule.Timezone)
}

func TestTaskRouter_UpdateSchedule(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Clean up test data
	cleanupDatabase(t, db)

	// Insert test task
	taskID := insertTestTaskDefinition(t, db, "Task With Schedule", "echo Scheduled")

	// Insert schedule
	scheduleID := insertTestSchedule(t, db, taskID, "0 0 * * *", "UTC")

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create the schedule update request
	updateSchedule := api.UpdateTaskScheduleRequest{
		CronExpression: "0 */2 * * *",
		Timezone:       "Asia/Tokyo",
	}

	// Convert request to JSON
	scheduleJSON, err := json.Marshal(updateSchedule)
	require.NoError(t, err)

	// Create a request to the endpoint
	endpoint := fmt.Sprintf("/definitions/%d/schedules/%d", taskID, scheduleID)
	req, err := http.NewRequest("PUT", endpoint, bytes.NewBuffer(scheduleJSON))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	// Add IDs to route parameters
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", fmt.Sprint(taskID))
	rctx.URLParams.Add("schId", fmt.Sprint(scheduleID))
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.UpdateSchedule(rr, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse the response
	var response models.TaskSchedule
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)

	// Verify the response
	assert.Equal(t, scheduleID, response.ID)
	assert.Equal(t, taskID, response.TaskID)
	assert.Equal(t, "0 */2 * * *", response.CronExpression)
	assert.Equal(t, "Asia/Tokyo", response.Timezone)

	// Verify schedule was actually updated in database
	var dbSchedule models.TaskSchedule
	err = db.Get(&dbSchedule, "SELECT * FROM task.schedule WHERE id = $1", scheduleID)
	require.NoError(t, err)
	assert.Equal(t, "0 */2 * * *", dbSchedule.CronExpression)
	assert.Equal(t, "Asia/Tokyo", dbSchedule.Timezone)
}

func TestTaskRouter_DeleteSchedule(t *testing.T) {
	// Setup database connection
	conf, err := config.LoadConfig()
	require.NoError(t, err)

	db, err := database.New(conf)
	require.NoError(t, err)
	defer mustClose(db)

	// Clean up test data
	cleanupDatabase(t, db)

	// Insert test task
	taskID := insertTestTaskDefinition(t, db, "Task With Schedule", "echo Scheduled")

	// Insert schedule
	scheduleID := insertTestSchedule(t, db, taskID, "0 0 * * *", "UTC")

	// Create router
	ctx := context.Background()
	router := chi.NewRouter()
	taskRouter := api.NewTaskRouter(ctx, db, router)

	// Create a request to the endpoint
	endpoint := fmt.Sprintf("/definitions/%d/schedules/%d", taskID, scheduleID)
	req, err := http.NewRequest("DELETE", endpoint, nil)
	require.NoError(t, err)

	// Add IDs to route parameters
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", fmt.Sprint(taskID))
	rctx.URLParams.Add("schId", fmt.Sprint(scheduleID))
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	// Set up response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	taskRouter.DeleteSchedule(rr, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse the response
	var response api.DeleteTaskScheduleResponse
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)

	// Verify the response
	assert.Equal(t, taskID, response.TaskID)
	assert.Equal(t, scheduleID, response.ScheduleID)
	assert.Contains(t, response.Message, fmt.Sprintf("%d", scheduleID))

	// Verify schedule was actually deleted from database
	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM task.schedule WHERE id = $1", scheduleID)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "Schedule should be deleted")
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
