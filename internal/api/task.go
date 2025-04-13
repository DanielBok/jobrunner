package api

import (
	"context"
	"fmt"
	"net/http"

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
	r.router.Get("/definitions", r.GetDefinitions)
	r.router.Post("/definitions", r.AddDefinition)

	return r
}

func (t *TaskRouter) getDefinition(id int64) (*ListTaskDefinition, error) {
	tx, err := t.db.Beginx()
	if err != nil {
		return nil, err
	}
	defer releaseTx(tx)

	var dest ListTaskDefinition
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
	definitionsMap := make(map[int64]*ListTaskDefinition)
	for i := range definitions {
		def := definitions[i]
		definitionsMap[def.ID] = &ListTaskDefinition{
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

func (t *TaskRouter) AddDefinition(w http.ResponseWriter, r *http.Request) {
	var payload CreateTaskDefinition

	if err := readJson(w, r, &payload); err != nil {
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
