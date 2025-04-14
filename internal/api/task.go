package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"jobrunner/internal/models"
)

type TaskRouter struct {
	ctx    context.Context
	db     *sqlx.DB
	router chi.Router
}

func (t *TaskRouter) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	t.router.ServeHTTP(writer, request)
}

func NewTaskRouter(ctx context.Context, db *sqlx.DB, router chi.Router) *TaskRouter {
	r := &TaskRouter{
		ctx:    ctx,
		db:     db,
		router: router,
	}
	// manage definitions
	r.router.Get("/definitions", r.GetDefinitions)           // list all definitions
	r.router.Post("/definitions", r.AddDefinition)           // add new definition
	r.router.Put("/definitions/{id}", r.UpdateDefinition)    // update definitions
	r.router.Delete("/definitions/{id}", r.DeleteDefinition) // delete definitions

	// manage dependencies for a particular definition
	r.router.Get("/definitions/{id}/dependencies/", r.GetDependencies)            // get all dependencies for
	r.router.Post("/definitions/{id}/dependencies", r.AddDependency)              // add dependency to definition
	r.router.Put("/definitions/{id}/dependencies/{depId}", r.UpdateDependency)    // update dependency in definition
	r.router.Delete("/definitions/{id}/dependencies/{depId}", r.DeleteDependency) // delete dependency from definition

	// manage schedules for a particular definition
	r.router.Get("/definitions/{id}/schedules/", r.GetSchedules)             // get all schedules for
	r.router.Post("/definitions/{id}/schedules", r.AddSchedule)              // add schedule to definition
	r.router.Put("/definitions/{id}/schedules/{schId}", r.UpdateSchedule)    // update schedule in definition
	r.router.Delete("/definitions/{id}/schedules/{schId}", r.DeleteSchedule) // delete schedule from definition

	return r
}

func (t *TaskRouter) getDefinition(id int64) (*TaskDefinitionResponse, error) {
	tx, err := t.db.Beginx()
	if err != nil {
		return nil, err
	}
	defer releaseTx(tx)

	var dest TaskDefinitionResponse
	if err := tx.GetContext(t.ctx, &dest, `SELECT * FROM task.definition WHERE id = $1`, id); err != nil {
		return nil, err
	}

	if err := tx.SelectContext(t.ctx, &dest.Dependencies, ` SELECT * FROM task.dependency WHERE task_id = $1 ORDER BY id`, id); err != nil {
		return nil, err
	}

	if err := t.db.SelectContext(t.ctx, &dest.Schedules, ` SELECT * FROM task.schedule WHERE task_id = $1 ORDER BY id`, id); err != nil {
		return nil, err
	}

	return &dest, nil
}

// GetDefinitions returns all information on the task definitions, their dependencies and schedules.
func (t *TaskRouter) GetDefinitions(w http.ResponseWriter, _ *http.Request) {
	tx, err := t.db.Beginx()
	if err != nil {
		http.Error(w, "Could not begin transaction", http.StatusInternalServerError)
		return
	}
	defer releaseTx(tx)

	var definitions []models.TaskDefinition
	if err := tx.SelectContext(t.ctx, &definitions, ` SELECT * FROM task.definition ORDER BY id `); err != nil {
		http.Error(w, "Failed to fetch task definitions", http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to fetch task definitions")
		return
	}
	definitionsMap := make(map[int64]*TaskDefinitionResponse)
	for i := range definitions {
		def := definitions[i]
		definitionsMap[def.ID] = &TaskDefinitionResponse{
			TaskDefinition: def,
			Schedules:      []*models.TaskSchedule{},
			Dependencies:   []*models.TaskDependency{},
		}
	}

	var schedules []models.TaskSchedule
	if err := t.db.SelectContext(t.ctx, &schedules, ` SELECT * FROM task.schedule ORDER BY task_id, id `); err != nil {
		http.Error(w, "Failed to fetch task schedules", http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to fetch task schedules")
		return
	}

	for i := range schedules {
		s := &schedules[i]
		definitionsMap[s.TaskID].Schedules = append(definitionsMap[s.TaskID].Schedules, s)
	}

	var dependencies []models.TaskDependency
	if err := tx.SelectContext(t.ctx, &dependencies, `
        SELECT * FROM task.dependency
        ORDER BY task_id, id
    `); err != nil {
		http.Error(w, "Failed to fetch task dependencies", http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to fetch task dependencies")
	}

	for i := range dependencies {
		d := &dependencies[i]
		definitionsMap[d.TaskID].Dependencies = append(definitionsMap[d.TaskID].Dependencies, d)
	}

	serveJson(w, definitions)
}

// AddDefinition adds a new task definition
func (t *TaskRouter) AddDefinition(w http.ResponseWriter, r *http.Request) {
	var payload CreateTaskDefinitionRequest

	if err := readJson(w, r, &payload); err != nil {
		return
	}

	if err := payload.Validate(); err != nil {
		http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
		return
	}

	tx, err := t.db.Beginx()
	if err != nil {
		http.Error(w, "Could not begin transaction", http.StatusInternalServerError)
		return
	}
	defer releaseTx(tx)

	var definitionID int64

	// add definition
	if err := tx.QueryRowContext(t.ctx,
		`INSERT INTO task.definition (name, description, image_name, command, timeout_seconds, max_retries, is_active) 
VALUES ($1, $2, $3, $4, $5, $6, TRUE)
RETURNING id`,
		payload.Name, payload.Description, payload.ImageName, payload.Command, payload.TimeoutSeconds, payload.MaxRetries,
	).Scan(&definitionID); err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not insert new definition: %v", err), http.StatusBadRequest)
		return
	}

	// add schedule
	for _, s := range payload.Schedules {
		if _, err := tx.ExecContext(t.ctx,
			`INSERT INTO task.schedule (task_id, cron_expression, timezone) 
VALUES ($1, $2, $3)`,
			definitionID, s.CronExpression, s.Timezone,
		); err != nil {
			rollbackTx(tx)
			http.Error(w, fmt.Sprintf("Could not insert new definition's schedule: %v", err), http.StatusBadRequest)
			return
		}
	}

	// add dependency
	for _, d := range payload.Dependencies {
		if _, err := tx.ExecContext(t.ctx,
			`INSERT INTO task.dependency (task_id, depends_on, lookback_window, min_wait_time, required_condition) 
VALUES ($1, $2, $3, $4, $5)`,
			definitionID, d.DependsOn, d.LookbackWindow, d.MinWaitTime, d.RequiredCondition); err != nil {
			rollbackTx(tx)
			http.Error(w, fmt.Sprintf("Could not insert new definition's schedule: %v", err), http.StatusBadRequest)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not insert new definition: %v", err), http.StatusBadRequest)
		return
	}

	definition, err := t.getDefinition(definitionID)
	if err != nil {
		http.Error(w, "Insertion was successful but could not get new definition", http.StatusInternalServerError)
		return
	}
	serveJson(w, definition)
}

// UpdateDefinition updates an existing task definition
func (t *TaskRouter) UpdateDefinition(w http.ResponseWriter, r *http.Request) {
	// Parse task ID from URL
	taskID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	// Parse request body
	var payload UpdateTaskDefinitionRequest
	if err := readJson(w, r, &payload); err != nil {
		http.Error(w, fmt.Sprintf("Could not parse json: %v", err), http.StatusBadRequest)
		return
	}
	if err := payload.Validate(); err != nil {
		http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
		return
	}

	// Begin transaction
	tx, err := t.db.Beginx()
	if err != nil {
		http.Error(w, "Could not begin transaction", http.StatusInternalServerError)
		return
	}
	defer releaseTx(tx)

	// Check if task exists
	var exists bool
	err = tx.QueryRowContext(t.ctx, "SELECT EXISTS(SELECT 1 FROM task.definition WHERE id = $1)", taskID).Scan(&exists)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error checking if task exists: %v", err), http.StatusInternalServerError)
		return
	}

	if !exists {
		rollbackTx(tx)
		http.Error(w, "Task definition not found", http.StatusNotFound)
		return
	}

	// Update the task definition
	_, err = tx.ExecContext(t.ctx, `
		UPDATE task.definition 
		SET name = $1, description = $2, image_name = $3, command = $4, 
			timeout_seconds = $5, max_retries = $6, is_active = $7
		WHERE id = $8
	`, payload.Name, payload.Description, payload.ImageName, payload.Command,
		payload.TimeoutSeconds, payload.MaxRetries, payload.IsActive, taskID)

	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not update task definition: %v", err), http.StatusInternalServerError)
		return
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not commit transaction: %v", err), http.StatusInternalServerError)
		return
	}

	// Return the updated task
	definition, err := t.getDefinition(taskID)
	if err != nil {
		http.Error(w, "Update was successful but could not get updated definition", http.StatusInternalServerError)
		return
	}
	serveJson(w, definition)
}

// DeleteDefinition deletes a task definition and all associated schedules and dependencies
func (t *TaskRouter) DeleteDefinition(w http.ResponseWriter, r *http.Request) {
	// Parse task ID from URL
	taskID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	// Begin transaction
	tx, err := t.db.Beginx()
	if err != nil {
		http.Error(w, "Could not begin transaction", http.StatusInternalServerError)
		return
	}
	defer releaseTx(tx)

	// Check if task exists
	var exists bool
	err = tx.QueryRowContext(t.ctx, "SELECT EXISTS(SELECT 1 FROM task.definition WHERE id = $1)", taskID).Scan(&exists)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error checking if task exists: %v", err), http.StatusInternalServerError)
		return
	}

	if !exists {
		rollbackTx(tx)
		http.Error(w, "Task definition not found", http.StatusNotFound)
		return
	}

	// Delete the task definition (cascade will delete schedules and dependencies)
	_, err = tx.ExecContext(t.ctx, "DELETE FROM task.definition WHERE id = $1", taskID)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not delete task definition: %v", err), http.StatusInternalServerError)
		return
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not commit transaction: %v", err), http.StatusInternalServerError)
		return
	}

	serveJson(w, DeleteTaskDefinitionResponse{
		TaskID:  taskID,
		Message: fmt.Sprintf("Task definition with ID %d successfully deleted", taskID),
	})
}

// GetDependencies returns all dependencies for a task definition
func (t *TaskRouter) GetDependencies(w http.ResponseWriter, r *http.Request) {
	// Parse task ID from URL
	taskID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	// Check if task exists
	var exists bool
	err = t.db.QueryRowContext(t.ctx, "SELECT EXISTS(SELECT 1 FROM task.definition WHERE id = $1)", taskID).Scan(&exists)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error checking if task exists: %v", err), http.StatusInternalServerError)
		return
	}

	if !exists {
		http.Error(w, "Task definition not found", http.StatusNotFound)
		return
	}

	// Get all dependencies for the task
	var dependencies []*models.TaskDependency
	err = t.db.SelectContext(t.ctx, &dependencies,
		"SELECT * FROM task.dependency WHERE task_id = $1 ORDER BY id", taskID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Could not fetch dependencies: %v", err), http.StatusInternalServerError)
		return
	}

	serveJson(w, dependencies)
}

// AddDependency adds a new dependency to a task definition
func (t *TaskRouter) AddDependency(w http.ResponseWriter, r *http.Request) {
	// Parse task ID from URL
	idParam := chi.URLParam(r, "id")
	taskID, err := strconv.ParseInt(idParam, 10, 64)
	if err != nil {
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	// Parse request body
	var payload CreateTaskDependencyRequest
	if err := readJson(w, r, &payload); err != nil {
		http.Error(w, fmt.Sprintf("Could not parse json: %v", err), http.StatusBadRequest)
		return
	}
	if err := payload.Validate(); err != nil {
		http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
		return
	}

	// Begin transaction
	tx, err := t.db.Beginx()
	if err != nil {
		http.Error(w, "Could not begin transaction", http.StatusInternalServerError)
		return
	}
	defer releaseTx(tx)

	// Check if task exists
	var exists bool
	err = tx.QueryRowContext(t.ctx, "SELECT EXISTS(SELECT 1 FROM task.definition WHERE id = $1)", taskID).Scan(&exists)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error checking if task exists: %v", err), http.StatusInternalServerError)
		return
	}

	if !exists {
		rollbackTx(tx)
		http.Error(w, "Task definition not found", http.StatusNotFound)
		return
	}

	// Check if dependency task exists
	err = tx.QueryRowContext(t.ctx, "SELECT EXISTS(SELECT 1 FROM task.definition WHERE id = $1)", payload.DependsOn).Scan(&exists)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error checking if dependency task exists: %v", err), http.StatusInternalServerError)
		return
	}

	if !exists {
		rollbackTx(tx)
		http.Error(w, "Dependency task not found", http.StatusBadRequest)
		return
	}

	// Prevent self-dependencies
	if taskID == payload.DependsOn {
		rollbackTx(tx)
		http.Error(w, "Task cannot depend on itself", http.StatusBadRequest)
		return
	}

	// Check for duplicate dependency
	err = tx.QueryRowContext(t.ctx,
		"SELECT EXISTS(SELECT 1 FROM task.dependency WHERE task_id = $1 AND depends_on = $2)",
		taskID, payload.DependsOn).Scan(&exists)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error checking for duplicate dependency: %v", err), http.StatusInternalServerError)
		return
	}

	if exists {
		rollbackTx(tx)
		http.Error(w, "Dependency already exists", http.StatusBadRequest)
		return
	}

	// Create new dependency
	var dependencyID int64
	err = tx.QueryRowContext(t.ctx, `
		INSERT INTO task.dependency (task_id, depends_on, lookback_window, min_wait_time, required_condition)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, taskID, payload.DependsOn, payload.LookbackWindow, payload.MinWaitTime, payload.RequiredCondition).Scan(&dependencyID)

	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not create dependency: %v", err), http.StatusInternalServerError)
		return
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not commit transaction: %v", err), http.StatusInternalServerError)
		return
	}

	// Get the newly created dependency
	var dependency models.TaskDependency
	err = t.db.GetContext(t.ctx, &dependency, "SELECT * FROM task.dependency WHERE id = $1", dependencyID)
	if err != nil {
		http.Error(w, "Dependency created but could not fetch details", http.StatusInternalServerError)
		return
	}

	serveJson(w, dependency)
}

// UpdateDependency updates an existing task dependency
func (t *TaskRouter) UpdateDependency(w http.ResponseWriter, r *http.Request) {
	// Parse task ID and dependency ID from URL
	taskID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	depID, err := strconv.ParseInt(chi.URLParam(r, "depId"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid dependency ID", http.StatusBadRequest)
		return
	}

	// Parse request body
	var payload UpdateTaskDependencyRequest
	if err := readJson(w, r, &payload); err != nil {
		http.Error(w, fmt.Sprintf("Could not parse json: %v", err), http.StatusBadRequest)
		return
	}
	if err := payload.Validate(); err != nil {
		http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
		return
	}

	// Begin transaction
	tx, err := t.db.Beginx()
	if err != nil {
		http.Error(w, "Could not begin transaction", http.StatusInternalServerError)
		return
	}
	defer releaseTx(tx)

	// Check if dependency exists and belongs to the task
	var exists bool
	err = tx.QueryRowContext(t.ctx,
		"SELECT EXISTS(SELECT 1 FROM task.dependency WHERE id = $1 AND task_id = $2)",
		depID, taskID).Scan(&exists)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error checking if dependency exists: %v", err), http.StatusInternalServerError)
		return
	}

	if !exists {
		rollbackTx(tx)
		http.Error(w, "Dependency not found or does not belong to this task", http.StatusNotFound)
		return
	}

	// Update the dependency
	_, err = tx.ExecContext(t.ctx, `
		UPDATE task.dependency
		SET lookback_window = $1, min_wait_time = $2, required_condition = $3
		WHERE id = $4 AND task_id = $5
	`, payload.LookbackWindow, payload.MinWaitTime, payload.RequiredCondition, depID, taskID)

	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not update dependency: %v", err), http.StatusInternalServerError)
		return
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not commit transaction: %v", err), http.StatusInternalServerError)
		return
	}

	// Get the updated dependency
	var dependency models.TaskDependency
	err = t.db.GetContext(t.ctx, &dependency, "SELECT * FROM task.dependency WHERE id = $1", depID)
	if err != nil {
		http.Error(w, "Update was successful but could not fetch updated dependency", http.StatusInternalServerError)
		return
	}

	serveJson(w, dependency)
}

// DeleteDependency removes a dependency from a task
func (t *TaskRouter) DeleteDependency(w http.ResponseWriter, r *http.Request) {
	// Parse task ID and dependency ID from URL
	taskID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	depID, err := strconv.ParseInt(chi.URLParam(r, "depId"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid dependency ID", http.StatusBadRequest)
		return
	}

	// Begin transaction
	tx, err := t.db.Beginx()
	if err != nil {
		http.Error(w, "Could not begin transaction", http.StatusInternalServerError)
		return
	}
	defer releaseTx(tx)

	// Check if dependency exists and belongs to the task
	var exists bool
	err = tx.QueryRowContext(t.ctx,
		"SELECT EXISTS(SELECT 1 FROM task.dependency WHERE id = $1 AND task_id = $2)",
		depID, taskID).Scan(&exists)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error checking if dependency exists: %v", err), http.StatusInternalServerError)
		return
	}

	if !exists {
		rollbackTx(tx)
		http.Error(w, "Dependency not found or does not belong to this task", http.StatusNotFound)
		return
	}

	// Delete the dependency
	result, err := tx.ExecContext(t.ctx, "DELETE FROM task.dependency WHERE id = $1 AND task_id = $2", depID, taskID)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not delete dependency: %v", err), http.StatusInternalServerError)
		return
	}

	rows, err := result.RowsAffected()
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error getting affected rows: %v", err), http.StatusInternalServerError)
		return
	}

	if rows == 0 {
		rollbackTx(tx)
		http.Error(w, "No rows were deleted", http.StatusInternalServerError)
		return
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not commit transaction: %v", err), http.StatusInternalServerError)
		return
	}

	// Return success message
	serveJson(w, DeleteTaskDependencyResponse{
		TaskID:       taskID,
		DependencyId: depID,
		Message:      fmt.Sprintf("Dependency with ID %d successfully deleted from task %d", depID, taskID),
	})
}

// GetSchedules returns all schedules for a task definition
func (t *TaskRouter) GetSchedules(w http.ResponseWriter, r *http.Request) {
	// Parse task ID from URL
	taskID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	// Check if task exists
	var exists bool
	err = t.db.QueryRowContext(t.ctx, "SELECT EXISTS(SELECT 1 FROM task.definition WHERE id = $1)", taskID).Scan(&exists)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error checking if task exists: %v", err), http.StatusInternalServerError)
		return
	}

	if !exists {
		http.Error(w, "Task definition not found", http.StatusNotFound)
		return
	}

	// Get all schedules for the task
	var schedules []*models.TaskSchedule
	err = t.db.SelectContext(t.ctx, &schedules, "SELECT * FROM task.schedule WHERE task_id = $1 ORDER BY id", taskID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Could not fetch schedules: %v", err), http.StatusInternalServerError)
		return
	}

	serveJson(w, schedules)
}

// AddSchedule adds a new schedule to a task definition
func (t *TaskRouter) AddSchedule(w http.ResponseWriter, r *http.Request) {
	// Parse task ID from URL
	taskID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	// Parse request body
	var payload CreateTaskScheduleRequest
	if err := readJson(w, r, &payload); err != nil {
		http.Error(w, fmt.Sprintf("Could not parse json: %v", err), http.StatusBadRequest)
		return
	}
	if err := payload.Validate(); err != nil {
		http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
		return
	}

	// Begin transaction
	tx, err := t.db.Beginx()
	if err != nil {
		http.Error(w, "Could not begin transaction", http.StatusInternalServerError)
		return
	}
	defer releaseTx(tx)

	// Check if task exists
	var exists bool
	err = tx.QueryRowContext(t.ctx, "SELECT EXISTS(SELECT 1 FROM task.definition WHERE id = $1)", taskID).Scan(&exists)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error checking if task exists: %v", err), http.StatusInternalServerError)
		return
	}

	if !exists {
		rollbackTx(tx)
		http.Error(w, "Task definition not found", http.StatusNotFound)
		return
	}

	// Create new schedule
	var scheduleID int64
	err = tx.QueryRowContext(t.ctx, `
		INSERT INTO task.schedule (task_id, cron_expression, timezone)
		VALUES ($1, $2, $3)
		RETURNING id
	`, taskID, payload.CronExpression, payload.Timezone).Scan(&scheduleID)

	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not create schedule: %v", err), http.StatusInternalServerError)
		return
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not commit transaction: %v", err), http.StatusInternalServerError)
		return
	}

	// Get the newly created schedule
	var schedule models.TaskSchedule
	err = t.db.GetContext(t.ctx, &schedule, "SELECT * FROM task.schedule WHERE id = $1", scheduleID)
	if err != nil {
		http.Error(w, "Schedule created but could not fetch details", http.StatusInternalServerError)
		return
	}

	serveJson(w, schedule)
}

// UpdateSchedule updates an existing task schedule
func (t *TaskRouter) UpdateSchedule(w http.ResponseWriter, r *http.Request) {
	// Parse task ID and schedule ID from URL
	taskID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	schID, err := strconv.ParseInt(chi.URLParam(r, "schId"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid schedule ID", http.StatusBadRequest)
		return
	}

	// Parse request body
	var payload UpdateTaskScheduleRequest
	if err := readJson(w, r, &payload); err != nil {
		http.Error(w, fmt.Sprintf("Could not parse json: %v", err), http.StatusBadRequest)
		return
	}
	if err := payload.Validate(); err != nil {
		http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
		return
	}

	// Begin transaction
	tx, err := t.db.Beginx()
	if err != nil {
		http.Error(w, "Could not begin transaction", http.StatusInternalServerError)
		return
	}
	defer releaseTx(tx)

	// Check if schedule exists and belongs to the task
	var exists bool
	err = tx.QueryRowContext(t.ctx,
		"SELECT EXISTS(SELECT 1 FROM task.schedule WHERE id = $1 AND task_id = $2)",
		schID, taskID).Scan(&exists)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error checking if schedule exists: %v", err), http.StatusInternalServerError)
		return
	}

	if !exists {
		rollbackTx(tx)
		http.Error(w, "Schedule not found or does not belong to this task", http.StatusNotFound)
		return
	}

	// Update the schedule
	_, err = tx.ExecContext(t.ctx, `
		UPDATE task.schedule
		SET cron_expression = $1, timezone = $2
		WHERE id = $3 AND task_id = $4
	`, payload.CronExpression, payload.Timezone, schID, taskID)

	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not update schedule: %v", err), http.StatusInternalServerError)
		return
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not commit transaction: %v", err), http.StatusInternalServerError)
		return
	}

	// Get the updated schedule
	var schedule models.TaskSchedule
	err = t.db.GetContext(t.ctx, &schedule, "SELECT * FROM task.schedule WHERE id = $1", schID)
	if err != nil {
		http.Error(w, "Update was successful but could not fetch updated schedule", http.StatusInternalServerError)
		return
	}

	serveJson(w, schedule)
}

// DeleteSchedule removes a schedule from a task
func (t *TaskRouter) DeleteSchedule(w http.ResponseWriter, r *http.Request) {
	// Parse task ID and schedule ID from URL
	taskID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	schID, err := strconv.ParseInt(chi.URLParam(r, "schId"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid schedule ID", http.StatusBadRequest)
		return
	}

	// Begin transaction
	tx, err := t.db.Beginx()
	if err != nil {
		http.Error(w, "Could not begin transaction", http.StatusInternalServerError)
		return
	}
	defer releaseTx(tx)

	// Check if schedule exists and belongs to the task
	var exists bool
	err = tx.QueryRowContext(t.ctx,
		"SELECT EXISTS(SELECT 1 FROM task.schedule WHERE id = $1 AND task_id = $2)",
		schID, taskID).Scan(&exists)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error checking if schedule exists: %v", err), http.StatusInternalServerError)
		return
	}

	if !exists {
		rollbackTx(tx)
		http.Error(w, "Schedule not found or does not belong to this task", http.StatusNotFound)
		return
	}

	// Count existing schedules for this task
	var scheduleCount int
	err = tx.QueryRowContext(t.ctx,
		"SELECT COUNT(*) FROM task.schedule WHERE task_id = $1", taskID).Scan(&scheduleCount)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error counting schedules: %v", err), http.StatusInternalServerError)
		return
	}

	// Delete the schedule
	result, err := tx.ExecContext(t.ctx, "DELETE FROM task.schedule WHERE id = $1 AND task_id = $2", schID, taskID)
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not delete schedule: %v", err), http.StatusInternalServerError)
		return
	}

	rows, err := result.RowsAffected()
	if err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Error getting affected rows: %v", err), http.StatusInternalServerError)
		return
	}

	if rows == 0 {
		rollbackTx(tx)
		http.Error(w, "No rows were deleted", http.StatusInternalServerError)
		return
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		rollbackTx(tx)
		http.Error(w, fmt.Sprintf("Could not commit transaction: %v", err), http.StatusInternalServerError)
		return
	}

	// Return success message
	serveJson(w, DeleteTaskScheduleResponse{
		TaskID:     taskID,
		ScheduleID: schID,
		Message:    fmt.Sprintf("Schedule with ID %d successfully deleted from task %d", schID, taskID),
	})
}
