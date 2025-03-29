package scheduler

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"
	"jobrunner/internal/models"
	"jobrunner/internal/queue"
)

type ScheduledCronJob struct {
	EntryID cron.EntryID
	Job     Schedule
}

type Scheduler struct {
	db               *sqlx.DB
	cron             *cron.Cron
	depProbe         *DependencyProbe
	scheduleIDMap    map[int64]ScheduledCronJob // the key is the ScheduleID
	scheduleMapMutex sync.RWMutex

	// Used for refresh operations
	isRunning  bool // checks if start has been called
	ticker     *time.Ticker
	context    context.Context
	cancelFunc context.CancelFunc
}

// NewScheduler creates a new scheduler service
func NewScheduler(db *sqlx.DB, queue queue.Client) *Scheduler {
	// Create cron with seconds precision
	c := cron.New(
		cron.WithParser(cron.NewParser(cron.SecondOptional|cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow)),
		cron.WithLocation(time.UTC),
	)

	return &Scheduler{
		db:               db,
		cron:             c,
		depProbe:         NewDependencyProbe(db, queue),
		scheduleIDMap:    make(map[int64]ScheduledCronJob),
		scheduleMapMutex: sync.RWMutex{},
		isRunning:        false,
	}
}

// Start begins the scheduler service
func (s *Scheduler) Start(ctx context.Context) error {
	if s.isRunning {
		return nil
	}

	s.isRunning = true
	s.context, s.cancelFunc = context.WithCancel(ctx)

	// Load all schedules
	if err := s.RefreshSchedules(s.context); err != nil {
		return err
	}

	s.startScheduleJobsRefresh(s.context, 60*time.Second) // Refresh every minute
	s.cron.Start()

	// check dependencies resolution
	s.depProbe.Start(s.context)

	return nil
}

// Stop stops the scheduler service
func (s *Scheduler) Stop() {
	if !s.isRunning {
		return
	}

	s.cancelFunc()
	if s.ticker != nil {
		s.ticker.Stop()
	}

	s.cron.Stop()
	s.depProbe.Stop()
	s.isRunning = false
}

func (s *Scheduler) startScheduleJobsRefresh(ctx context.Context, interval time.Duration) {
	s.ticker = time.NewTicker(interval)

	go func() {
		isRunning := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.context.Done():
				return
			case <-s.ticker.C:
				if isRunning {
					continue
				}

				isRunning = true
				go func() {
					defer func() { isRunning = false }()
					if err := s.RefreshSchedules(ctx); err != nil {
						log.Error().Err(err).Msg("Failed to refresh schedules")
					}
				}()
			}
		}
	}()
}

// RefreshSchedules reloads all schedules and updates cron jobs
func (s *Scheduler) RefreshSchedules(ctx context.Context) error {
	log.Info().Msg("Refreshing job schedules...")

	//Get all schedules
	schedules, err := s.GetAllSchedules(ctx)
	if err != nil {
		return err
	}

	for _, schedule := range schedules {
		sc, exists := s.scheduleIDMap[schedule.ScheduleID]

		switch {
		case !exists && schedule.IsActive:
			// Does not exist and is active, add to scheduled cron job
			if err := s.AddSchedule(ctx, schedule); err != nil {
				return err
			}
		case !schedule.IsActive:
			// Job exists in cron but is no longer active, remove
			s.RemoveSchedule(schedule.ScheduleID)
		case !schedule.Equal(&sc.Job):
			// jobs are not the same even though they have the same ScheduleID
			// this means that the Schedule has been updated
			s.RemoveSchedule(schedule.ScheduleID)
			if err := s.AddSchedule(ctx, schedule); err != nil {
				return err
			}
		}
	}

	log.Info().Msg("Schedule refresh complete")
	return nil
}

// GetAllSchedules fetches all the schedules. Note that a single job can
// have multiple schedules.
func (s *Scheduler) GetAllSchedules(ctx context.Context) ([]Schedule, error) {
	var schedules []Schedule
	query := `
SELECT s.job_id,
       s.id AS schedule_id,
       j.name,
       j.image_name,
       j.command,
       j.timeout_seconds,
       j.max_retries,
       FORMAT('CRON_TZ=%s %s', s.timezone, s.cron_expression) AS cron_expression,
       j.is_active,
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
FROM tasks.job j
LEFT JOIN tasks.schedule s ON s.job_id = j.id
ORDER BY job_id, schedule_id`

	if err := s.db.SelectContext(ctx, &schedules, query); err != nil {
		return nil, err
	}

	for i := range schedules {
		schedule := &schedules[i]
		if schedule.DependencyJSON != nil {
			if err := json.Unmarshal(schedule.DependencyJSON, &schedule.Dependencies); err != nil {
				return nil, err
			}
		}
	}

	return schedules, nil
}

// AddSchedule adds a Schedule into the cron scheduler
func (s *Scheduler) AddSchedule(ctx context.Context, schedule Schedule) error {
	// Create a job-specific context that can be cancelled when the job is removed
	// Add the job to cron with the proper timezone
	entryID, err := s.cron.AddFunc(schedule.CronExpression, func() {
		if ctx.Err() != nil {
			return // Context cancelled
		}
		s.scheduleJobExecution(ctx, schedule)
	})

	if err != nil {
		log.Error().
			Err(err).
			Int64("job_id", schedule.JobID).
			Str("cron", schedule.CronExpression).
			Msg("Failed to schedule job")
		return err
	}

	// Store the entry ID
	s.scheduleMapMutex.Lock()
	s.scheduleIDMap[schedule.JobID] = ScheduledCronJob{entryID, schedule}
	s.scheduleMapMutex.Unlock()

	return nil
}

// RemoveSchedule removes a Schedule from the cron scheduler
func (s *Scheduler) RemoveSchedule(scheduleID int64) {
	s.scheduleMapMutex.Lock()
	defer s.scheduleMapMutex.Unlock()

	if sc, exists := s.scheduleIDMap[scheduleID]; exists {
		s.cron.Remove(sc.EntryID)
		delete(s.scheduleIDMap, scheduleID)
		log.Info().
			Int64("schedule_id", scheduleID).
			Msg("Removed job schedule")
	}
}

// scheduleJobExecution creates a new job execution record. After which, a message will be sent to
// a distributed queue system to inform worker nodes to pick up the task.
func (s *Scheduler) scheduleJobExecution(ctx context.Context, schedule Schedule) {
	// Insert execution record
	query := `
		INSERT INTO tasks.execution
		(job_id, status)
		VALUES ($1, $2)
		RETURNING id, created_at
	`

	var executionID int64
	var createdAt time.Time
	if err := s.db.QueryRowContext(ctx, query, schedule.JobID, models.EsPending).Scan(&executionID, &createdAt); err != nil {
		log.Error().
			Err(err).
			Int64("job_id", schedule.JobID).
			Msg("Failed to create job execution")
	}

	log.Info().
		Int64("job_id", schedule.JobID).
		Int64("execution_id", executionID).
		Msg("Job execution scheduled")
}
