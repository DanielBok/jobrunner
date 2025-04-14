package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/guregu/null/v6"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"jobrunner/internal/models"
	"jobrunner/internal/queue"
)

// DependencyProbe is used to check if a TaskSchedule has all its dependencies met
type DependencyProbe struct {
	db    *sqlx.DB     // Database connection
	queue queue.Client // Queue to pass message to worker nodes

	// Used for refresh operations
	isRunning  bool // checks if start has been called
	ticker     *time.Ticker
	context    context.Context
	cancelFunc context.CancelFunc
}

type TaskDependency struct {
	ID                int64                    `db:"id" json:"id"`                                // Task Dependency ID
	TaskID            int64                    `db:"task_id" json:"taskId"`                       // Task ID
	DependsOn         int64                    `db:"depends_on" json:"dependsOn"`                 // The parent which must be "completed" before this task can run
	LookbackWindow    int                      `db:"lookback_window" json:"lookbackWindow"`       // How far back (in seconds) to lookback for the "complete" condition
	RequiredCondition models.RequiredCondition `db:"required_condition" json:"requiredCondition"` // The type of completion required to say dependency is met
	MinWaitSeconds    int                      `db:"min_wait_seconds" json:"minWaitSeconds"`      // Extra time needed to wait after parent completion before task can run
}

// Equal checks if 2 TaskDependency are the same
func (j *TaskDependency) Equal(other *TaskDependency) bool {
	return j.ID == other.ID &&
		j.TaskID == other.TaskID &&
		j.DependsOn == other.DependsOn &&
		j.LookbackWindow == other.LookbackWindow &&
		j.RequiredCondition == other.RequiredCondition &&
		j.MinWaitSeconds == other.MinWaitSeconds
}

type PendingTask struct {
	ID             int64            `db:"id"`              // Execution ID
	TaskID         int64            `db:"task_id"`         // Task ID
	Command        string           `db:"command"`         // Command to run for task
	ImageName      null.String      `db:"image_name"`      // The docker image name. If provided, will run the task in docker
	TimeoutSeconds int64            `db:"timeout_seconds"` // The maximum time that the task can stay in the "running" execution status
	MaxRetries     int              `db:"max_retries"`     // Number of times the task can retry
	CreatedAt      time.Time        `db:"created_at"`      // Time the execution was created
	Dependencies   []TaskDependency `db:"-"`               // Task dependencies
	DependencyJSON []byte           `db:"dependencies"`
}

// NewDependencyProbe creates a new dependency resolver.
// db refers to a sqlx DB instance and queue refers to any implementation of the queue.Client
// interface. The interval argument determines how often the probe will check if all dependencies
// are met
func NewDependencyProbe(db *sqlx.DB, queue queue.Client) *DependencyProbe {
	return &DependencyProbe{
		db:        db,
		queue:     queue,
		isRunning: false,
	}
}

// Start will trigger the probe to scan continuously for tasks that are pending and
// check that their dependencies are all met. This function will run in its own
// goroutine
func (dp *DependencyProbe) Start(ctx context.Context) {
	if dp.isRunning {
		return
	}

	dp.isRunning = true
	dp.ticker = time.NewTicker(30 * time.Second)
	dp.context, dp.cancelFunc = context.WithCancel(ctx)

	go func() {
		isRunning := false

		for {
			select {
			case <-dp.context.Done():
				return
			case <-dp.ticker.C:
				if isRunning {
					continue
				}

				isRunning = true
				go func() {
					defer func() { isRunning = false }()
					if pendingTasks, err := dp.FetchPendingTasks(dp.context); err != nil {
						log.Error().Err(err).Msg("Error fetching pending tasks")
					} else {
						// Process each pending task
						for _, task := range pendingTasks {
							if err := dp.ProcessPendingTask(ctx, task); err != nil {
								log.Error().
									Err(err).
									Int64("run_id", task.ID).
									Int64("task_id", task.TaskID).
									Msg("Error processing task")
							}
						}
					}
				}()
			}
		}
	}()
}

func (dp *DependencyProbe) Stop() {
	if !dp.isRunning {
		return
	}

	dp.cancelFunc()
	if dp.ticker != nil {
		dp.ticker.Stop()
	}
	dp.isRunning = false
}

// FetchPendingTasks finds tasks with unsatisfied dependencies and checks if they're now satisfied.
// If they are satisfied, sends a message to the queue for the workers to start working on them.
// If they are not, check if they have "lapsed", gone past the maximum allowable wait time. If so,
// mark those tasks as lapsed.
func (dp *DependencyProbe) FetchPendingTasks(ctx context.Context) (pendingTasks []PendingTask, err error) {
	// Find pending executions with unsatisfied dependencies
	query := `
		SELECT r.id, 
		       r.task_id, 
		       d.command, 
		       d.image_name, 
		       d.timeout_seconds, 
		       d.max_retries, 
		       r.created_at,
		       COALESCE(
			       (SELECT JSONB_AGG(
						   JSONB_BUILD_OBJECT(
							   'id', dep.id,
							   'taskId', dep.task_id,
							   'dependsOn', dep.depends_on,
							   'lookbackWindow', dep.lookback_window,
							   'requiredCondition', dep.required_condition,
							   'minWaitSeconds', dep.min_wait_time
						   )
				       )
				   FROM task.dependency dep
				   WHERE dep.task_id = d.id), '[]'::JSONB
		       ) AS dependencies
		FROM task.run r
		JOIN task.definition d ON r.task_id = d.id
		WHERE r.status = 'pending'
	`

	err = dp.db.SelectContext(ctx, &pendingTasks, query)
	return
}

// ProcessPendingTask takes a task that is in pending state and processes it. In normal instances, this sends
// the task information to the queue where a worker will pick it up and execute it. The routine also checks if
// the task has "lapsed" (gone past its allowed start time). In those cases, it will mark the task as "lapsed"
// in the database and stop processing it.
func (dp *DependencyProbe) ProcessPendingTask(ctx context.Context, task PendingTask) error {
	if err := json.Unmarshal(task.DependencyJSON, &task.Dependencies); err != nil {
		log.Error().Err(err).Int64("run_id", task.ID).Msg("Failed to parse dependencies")
		return err
	}

	// Check if the task has gone pass the cutoff time. If so, mark the task as lapsed and don't work on it anymore
	// To derive the cutoff time, take the time the task was first scheduled (created_at) and add the dependency's
	// LookbackWindow (seconds). For example, if the execution request was first created at 01:00, and the
	// LookbackWindow is 3600 seconds, then the cutoff time is 02:00. If the dependency is still not met at 02:00,
	// then the task is marked as "lapsed" and will not be executed anymore.
	now := time.Now()
	for _, dep := range task.Dependencies {
		cutoffTime := task.CreatedAt.Add(time.Duration(dep.LookbackWindow) * time.Second)
		if now.After(cutoffTime) {
			query := `
				UPDATE task.run
				SET status = $1
				WHERE id = $2
			`
			_, err := dp.db.ExecContext(ctx, query, models.RunStatusLapsed, task.ID)
			if err != nil {
				log.Error().Err(err).Int64("run_id", task.ID).Msg("Failed to mark lapsed execution")
				return errors.New("could not mark task as lapsed")
			}

			return fmt.Errorf("could not mark task as 'lapsed'. %w", err)
		}
	}

	// Check if dependencies are now satisfied
	depsMet, err := dp.CheckDependencies(ctx, task.Dependencies)
	if err != nil {
		log.Error().Err(err).Int64("run_id", task.ID).Msg("Failed to check dependencies")
		return err
	}

	// If dependencies are met, send a message to the queue notifying that a task is ready to be done
	if depsMet {
		// Send to queue
		message := queue.TaskMessage{
			RunID:       task.ID,
			TaskID:      task.TaskID,
			Command:     task.Command,
			ImageName:   task.ImageName.String,
			Timeout:     task.TimeoutSeconds,
			MaxRetries:  task.MaxRetries,
			ScheduledAt: task.CreatedAt,
		}

		if err := dp.queue.Publish(ctx, message); err != nil {
			log.Error().Err(err).Int64("run_id", task.ID).Msg("Failed to publish to queue")
			return err
		}

		log.Info().Int64("run_id", task.ID).Msg("Dependencies satisfied, task published to queue")
	}
	return nil
}

// CheckDependencies verifies if all dependencies for a task execution are satisfied
func (dp *DependencyProbe) CheckDependencies(ctx context.Context, dependencies []TaskDependency) (bool, error) {
	if len(dependencies) == 0 {
		return true, nil
	}

	var params []interface{}
	var clauses []string
	depMap := make(map[int64]TaskDependency)
	for _, d := range dependencies {
		clauses = append(clauses, "(task_id = ? AND end_time > NOW() - MAKE_INTERVAL(secs => ?))")
		params = append(params, d.DependsOn, d.LookbackWindow)
		depMap[d.DependsOn] = d
	}

	query := dp.db.Rebind(fmt.Sprintf(`
	SELECT DISTINCT ON (task_id)
	    id, task_id, status, start_time, end_time, exit_code, output, error, attempts, worker_id
	FROM task.run
	WHERE %s
	ORDER BY task_id, end_time DESC NULLS LAST
	`, strings.Join(clauses, " OR ")))

	var parentExecutions []TaskRun
	err := dp.db.SelectContext(ctx, &parentExecutions, query, params...)
	if err != nil {
		return false, err
	}

	if len(parentExecutions) != len(dependencies) {
		return false, nil
	}

	now := time.Now()

	// Check that all task execution's exit code meets the
	for _, ex := range parentExecutions {
		parent := depMap[ex.TaskID]

		// If the EndTime is not Valid or the EndTime is before the latest acceptable time
		if !ex.EndTime.Valid ||
			ex.EndTime.Time.Before(now.Add(-time.Second*time.Duration(parent.LookbackWindow))) {
			return false, nil
		}

		if !metDependencyCondition(parent.RequiredCondition, ex.Status) {
			return false, nil
		}
	}

	return true, nil
}

func metDependencyCondition(condition models.RequiredCondition, status models.RunStatus) bool {
	return (condition == models.ReqCondSuccess && status == models.RunStatusCompleted) ||
		(condition == models.ReqCondCompletion && (status == models.RunStatusCompleted || status == models.RunStatusFailed)) ||
		(condition == models.ReqCondFailure && status == models.RunStatusFailed) ||
		(condition == models.ReqCondCancelled && (status == models.RunStatusCancelled)) ||
		(condition == models.ReqCondLapsed && status == models.RunStatusLapsed)
}
