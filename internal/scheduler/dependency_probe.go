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

// DependencyProbe is used to check if a JobSchedule has all its dependencies met
type DependencyProbe struct {
	db    *sqlx.DB     // Database connection
	queue queue.Client // Queue to pass message to worker nodes

	// Used for refresh operations
	isRunning  bool // checks if start has been called
	ticker     *time.Ticker
	context    context.Context
	cancelFunc context.CancelFunc
}

type JobDependency struct {
	ID                int64                    `db:"id" json:"id"`                                 // Job Dependency ID
	JobID             int64                    `db:"job_id" json:"job_id"`                         // Job ID
	DependsOn         int64                    `db:"depends_on" json:"depends_on"`                 // The parent which must be "completed" before this job can run
	LookbackWindow    int                      `db:"lookback_window" json:"lookback_window"`       // How far back (in seconds) to lookback for the "complete" condition
	RequiredCondition models.RequiredCondition `db:"required_condition" json:"required_condition"` // The type of completion required to say dependency is met
	MinWaitSeconds    int                      `db:"min_wait_seconds" json:"min_wait_seconds"`     // Extra time needed to wait after parent completion before job can run
}

type PendingJob struct {
	ID             int64           `db:"id"`              // Execution ID
	JobID          int64           `db:"job_id"`          // Job ID
	Command        string          `db:"command"`         // Command to run for job
	ImageName      null.String     `db:"image_name"`      // The docker image name. If provided, will run the task in docker
	TimeoutSeconds int             `db:"timeout_seconds"` // The maximum time that the job can stay in the "running" execution status
	MaxRetries     int             `db:"max_retries"`     // Number of times the job can retry
	CreatedAt      time.Time       `db:"created_at"`      // Time the execution was created
	Dependencies   []JobDependency `db:"-"`               // Job dependencies
	DependencyJSON []byte          `db:"dependencies"`
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

// Start will trigger the probe to scan continuously for jobs that are pending and
// check that their dependencies are all met
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
					if err := dp.checkPendingJobs(dp.context); err != nil {
						log.Error().Err(err).Msg("Error checking pending jobs")
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

// checkPendingJobs finds jobs with unsatisfied dependencies and checks if they're now satisfied.
// If they are satisfied, sends a message to the queue for the workers to start working on them.
// If they are not, check if they have "lapsed", gone past the maximum allowable wait time. If so,
// mark those jobs as lapsed.
func (dp *DependencyProbe) checkPendingJobs(ctx context.Context) error {
	// Find pending executions with unsatisfied dependencies
	query := `
		SELECT e.id, 
		       e.job_id, 
		       j.command, 
		       j.image_name, 
		       j.timeout_seconds, 
		       j.max_retries, 
		       e.created_at,
		       COALESCE(
			       (SELECT JSONB_AGG(
						   JSONB_BUILD_OBJECT(
							   'id', dep.id,
							   'job_id', dep.job_id,
							   'depends_on', dep.depends_on,
							   'lookback_window', dep.lookback_window,
							   'required_condition', dep.required_condition,
							   'min_wait_seconds', dep.min_wait_time
						   )
				       )
				   FROM tasks.dependency dep
				   WHERE dep.job_id = j.id), '[]'::jsonb
		       ) AS dependencies
		FROM tasks.execution e
		JOIN tasks.job j ON e.job_id = j.id
		WHERE e.status = 'pending'
	`

	var pendingJobs []PendingJob
	if err := dp.db.SelectContext(ctx, &pendingJobs, query); err != nil {
		return err
	}

	// Process each pending job
	for _, job := range pendingJobs {
		if err := dp.processPendingJob(ctx, job); err != nil {

		}
	}

	return nil
}

func (dp *DependencyProbe) processPendingJob(ctx context.Context, job PendingJob) error {
	if err := json.Unmarshal(job.DependencyJSON, &job.Dependencies); err != nil {
		log.Error().Err(err).Int64("execution_id", job.ID).Msg("Failed to parse dependencies")
		return err
	}

	// Check if the job has gone pass the cutoff time. If so, mark the job as lapsed and don't work on it anymore
	// To derive the cutoff time, take the time the job was first scheduled (created_at) and add the dependency's
	// LookbackWindow (seconds). For example, if the execution request was first created at 01:00, and the
	// LookbackWindow is 3600 seconds, then the cutoff time is 02:00. If the dependency is still not met at 02:00,
	// then the job is marked as "lapsed" and will not be executed anymore.
	now := time.Now()
	for _, dep := range job.Dependencies {
		cutoffTime := job.CreatedAt.Add(time.Duration(dep.LookbackWindow) * time.Second)
		if now.After(cutoffTime) {
			query := `
				UPDATE tasks.execution
				SET status = $1
				WHERE id = $2
			`
			_, err := dp.db.ExecContext(ctx, query, models.EsLapsed, job.ID)
			if err != nil {
				log.Error().Err(err).Int64("execution_id", job.ID).Msg("Failed to mark lapsed execution")
				return errors.New("could not mark job as lapsed")
			}

			return fmt.Errorf("could not mark job as 'lapsed'. %w", err)
		}
	}

	// Check if dependencies are now satisfied
	depsMet, err := dp.CheckDependencies(ctx, job.Dependencies)
	if err != nil {
		log.Error().Err(err).Int64("execution_id", job.ID).Msg("Failed to check dependencies")
		return err
	}

	// If dependencies are met, send a message to the queue notifying that a job is ready to be done
	if depsMet {
		// Send to queue
		message := queue.TaskMessage{
			ExecutionID: job.ID,
			JobID:       job.JobID,
			Command:     job.Command,
			ImageName:   job.ImageName.String,
			Timeout:     job.TimeoutSeconds,
			MaxRetries:  job.MaxRetries,
			ScheduledAt: job.CreatedAt,
		}

		if err := dp.queue.Publish(ctx, message); err != nil {
			log.Error().Err(err).Int64("execution_id", job.ID).Msg("Failed to publish to queue")
			return err
		}

		log.Info().Int64("execution_id", job.ID).Msg("Dependencies satisfied, job published to queue")
	}
	return nil
}

// CheckDependencies verifies if all dependencies for a job execution are satisfied
func (dp *DependencyProbe) CheckDependencies(ctx context.Context, dependencies []JobDependency) (bool, error) {
	if len(dependencies) == 0 {
		return true, nil
	}

	var params []interface{}
	var clauses []string
	depMap := make(map[int64]JobDependency)
	for _, d := range dependencies {
		clauses = append(clauses, "(job_id = ? AND end_time > NOW() - MAKE_INTERVAL(secs => ?))")
		params = append(params, d.DependsOn, d.LookbackWindow)
		depMap[d.DependsOn] = d
	}

	query := dp.db.Rebind(fmt.Sprintf(`
	SELECT DISTINCT ON (job_id)
	    id, job_id, status, start_time, end_time, exit_code, output, error, attempts, worker_id
	FROM tasks.execution
	WHERE %s
	ORDER BY job_id, end_time DESC NULLS LAST
	`, strings.Join(clauses, " OR ")))

	var parentExecutions []JobExecution
	err := dp.db.SelectContext(ctx, &parentExecutions, query, params...)
	if err != nil {
		return false, err
	}

	if len(parentExecutions) != len(dependencies) {
		return false, nil
	}

	now := time.Now()

	// Check that all job execution's exit code meets the
	for _, ex := range parentExecutions {
		parent := depMap[ex.JobId]

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

func metDependencyCondition(condition models.RequiredCondition, status models.ExecutionStatus) bool {
	switch {
	case condition == models.RcSuccess && status == models.EsCompleted:
	case condition == models.RcCompletion && (status == models.EsCompleted || status == models.EsFailed):
	case condition == models.RcFailure && status == models.EsFailed:
	case condition == models.RcCancelled && (status == models.EsCancelled):
	case condition == models.RcLapsed && status == models.EsLapsed:
		return true
	}
	return false
}
