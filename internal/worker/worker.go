package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"jobrunner/internal/queue"
)

type Worker struct {
	ID     string
	db     *sqlx.DB
	queue  queue.Client
	ctx    context.Context
	cancel context.CancelFunc
}

func NewWorker(db *sqlx.DB, queue queue.Client) Worker {
	id := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())
	return Worker{ID: id, db: db, queue: queue, ctx: ctx, cancel: cancel}
}

// Run is a blocking function. It starts to listen to the queue for any queue.TaskMessage.
// If there  are, it will execute the task as specified.
func (w *Worker) Run() error {
	return w.queue.Subscribe(w.ctx, func(message queue.TaskMessage) error {
		ctx, cancel := context.WithTimeout(w.ctx, time.Duration(message.Timeout)*time.Second)
		defer cancel()

		if _, err := tryRun(message.MaxRetries, func() error {
			return w.updateTaskStart(ctx, message.RunID)
		}); err != nil {
			return fmt.Errorf("could not start task: %w", err)
		}

		go w.sendTaskHeartbeat(ctx, message.RunID)

		numAttempts, runResult, lastErr := tryRunR(message.MaxRetries, func() (RunResult, error) {
			switch message.ImageName {
			case "":
				return w.RunDockerTask(message)
			default:
				return w.RunShellTask(message)
			}
		})

		_, err := tryRun(message.MaxRetries, func() error {
			_, err := w.db.ExecContext(ctx, `
UPDATE task.run
SET end_time = NOW(),
	exit_code = $2,
	output = $3,
	error = $4,
	attempts = $5
WHERE id = $1
`, message.TaskID, runResult.ExitCode, runResult.Output, runResult.Error, numAttempts)
			return err
		})
		if err != nil {
			return fmt.Errorf("could not update end of task: %w\n\nOriginal error:%w", err, lastErr)
		}

		return lastErr
	})
}

// tryRunR attempts to run a function for maxRetries time. If any time the function f succeeds,
// it will return the result and no error straightaway. Otherwise, it will return the zero value
// of the result type and the error
func tryRunR[R any](maxRetries int, f func() (R, error)) (numAttempts int, result R, lastErr error) {
	for i := 0; i < maxRetries; i++ {
		result, err := f()
		if err == nil {
			return i, result, nil
		}

		lastErr = err
		time.Sleep(time.Duration(i*100) * time.Millisecond) // Exponential backoff

	}
	return maxRetries, result, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

// tryRun attempts to run a function maxRetries time. If any time the function f succeeds,
// it will return with no error straightaway. Otherwise, it will return the error
func tryRun(maxRetries int, f func() error) (numAttempts int, lastErr error) {
	for i := 0; i < maxRetries; i++ {
		err := f()
		if err == nil {
			return i, nil
		}
		lastErr = err
		time.Sleep(time.Duration(i*100) * time.Millisecond) // Exponential backoff
	}

	return maxRetries, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

func (w *Worker) updateTaskStart(ctx context.Context, runID int64) error {
	_, err := w.db.ExecContext(ctx, `
UPDATE task.run
SET start_time = NOW(),
	status = 'running',
	attempts = 1,
	worker_id = $1
WHERE id = $2
`, w.ID, runID)

	return err
}

// sendTaskHeartbeat updates the task.run table to inform other folks that the task is still
// running. It sends the update every 1 minute. This allows the scheduler.TaskScheduler to
// identify tasks that have died and thus resend the task for work
func (w *Worker) sendTaskHeartbeat(ctx context.Context, runID int64) {
	ticker := time.NewTicker(time.Minute)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := w.db.ExecContext(w.ctx, `UPDATE task.run SET last_heartbeat = NOW() WHERE id = $1 `, runID)
			if err != nil {
				log.Error().
					Err(err).
					Int64("run_id", runID).
					Msg("Could not update task heartbeat")
			}
		}
	}

}

type RunResult struct {
	Output   string
	Error    string
	ExitCode int
}

func (w *Worker) RunDockerTask(message queue.TaskMessage) (RunResult, error) {
	var zero RunResult

	return zero, nil
}

func (w *Worker) RunShellTask(message queue.TaskMessage) (RunResult, error) {
	var zero RunResult

	return zero, nil
}

func (w *Worker) Stop() {
	w.cancel()
}
