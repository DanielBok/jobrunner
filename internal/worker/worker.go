package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"jobrunner/internal/models"
	"jobrunner/internal/queue"
)

type Worker struct {
	ID     string
	db     *sqlx.DB
	queue  queue.Client
	ctx    context.Context
	cancel context.CancelFunc
}

const (
	ExitCodeCancelled int = 990
	ExitCodeTimeOut   int = 991
	ExitCodeUnknown   int = 999
)

func New(db *sqlx.DB, queue queue.Client) *Worker {
	id := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{ID: id, db: db, queue: queue, ctx: ctx, cancel: cancel}
}

// Start is a blocking function. It starts to listen to the queue for any queue.TaskMessage.
// If there  are, it will execute the task as specified.
func (w *Worker) Start() error {
	return w.queue.Subscribe(w.ctx, func(message queue.TaskMessage) {
		ctx, cancel := context.WithTimeout(w.ctx, time.Duration(message.Timeout)*time.Second)
		defer cancel()

		// updates task start
		if _, err := tryRun(message.MaxRetries, func() error {
			return w.updateTaskStart(ctx, message.RunID)
		}); err != nil {
			log.Error().Err(err).Int64("run_id", message.RunID).Msg("Could not start task")
			return
		}

		// start a process to update the heartbeat so others know the task is getting worked on
		// this will end when the context gets cancelled at the end
		go w.sendTaskHeartbeat(ctx, message.RunID)
		go w.listenForTimeout(ctx, message.RunID)

		numAttempts, res, err := tryRunR(message.MaxRetries, func() (*RunResult, error) {
			switch message.ImageName {
			case "":
				return w.RunDockerTask(ctx, message)
			default:
				return w.RunShellTask(ctx, message)
			}
		})
		if err != nil {
			log.Error().Err(err).Int64("run_id", message.RunID).Msg("Could not execute task successfully")
		}

		if _, err := tryRun(message.MaxRetries, func() error {
			_, err := w.db.ExecContext(ctx, `
UPDATE task.run
SET end_time = NOW(),
	exit_code = $2,
	output = $3,
	error = $4,
	attempts = $5,
	status = $6
WHERE id = $1
`, message.TaskID, res.ExitCode, res.Output, res.Error, numAttempts, res.Status)
			return err
		}); err != nil {
			log.Error().Err(err).Int64("run_id", message.RunID).Msg("Could not update end of task")
		}
	})
}

// listenForTimeout checks if the run is cancelled because of a timeout. If so, it will log the error into the
// database. If not, it will just exit silently
func (w *Worker) listenForTimeout(ctx context.Context, runID int64) {
	<-ctx.Done()

	err := ctx.Err()
	if errors.Is(err, context.DeadlineExceeded) {
		_, err := w.db.Exec(`
UPDATE task.run
SET end_time = NOW(),
	exit_code = 1,
	error = 'TIMEOUT',
	status = $2
WHERE id = $1
`, runID, models.ReqCondFailure)

		if err != nil {
			log.Error().Err(err).Int64("run_id", runID).Msg("Could not update timeout error")
		}
	}
}

// tryRunR attempts to run a function for maxRetries time. If any time the function f succeeds,
// it will return the result and no error straightaway. Otherwise, it will return the zero value
// of the result type and the error
func tryRunR[R any](maxRetries int, f func() (R, error)) (numAttempts int, result R, lastErr error) {
	for attempts := 1; attempts-1 < maxRetries; attempts++ {
		result, err := f()
		if err == nil {
			return attempts, result, nil
		}

		lastErr = err
		time.Sleep(time.Duration(attempts) * 5 * time.Second) // Exponential backoff

	}
	return maxRetries, result, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

// tryRun attempts to run a function maxRetries time. If any time the function f succeeds,
// it will return with no error straightaway. Otherwise, it will return the error
func tryRun(maxRetries int, f func() error) (numAttempts int, lastErr error) {
	for attempts := 1; attempts-1 < maxRetries; attempts++ {
		err := f()
		if err == nil {
			return attempts, nil
		}
		lastErr = err
		time.Sleep(time.Duration(attempts) * 5 * time.Second) // Exponential backoff
	}

	return maxRetries, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

func (w *Worker) updateTaskStart(ctx context.Context, runID int64) error {
	_, err := w.db.ExecContext(ctx, `
UPDATE task.run
SET start_time = NOW(),
	status = $2,
	attempts = 1,
	worker_id = $3
WHERE id = $1
`, runID, w.ID, models.RunStatusRunning)

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
	Status   models.RunStatus
}

func (w *Worker) RunDockerTask(ctx context.Context, message queue.TaskMessage) (*RunResult, error) {
	log.Info().
		Str("type", "docker").
		Int64("run_id", message.RunID).
		Int64("task_id", message.TaskID).
		Str("command", message.Command).
		Msg("Executing task")

	// Container name
	containerName := fmt.Sprintf("jobrunner-%d-%s", message.RunID, w.ID)

	// Build docker run command
	args := []string{
		"run",
		// Removes a container after its stopped
		"--rm",
		// Add timeout with proper signal handling
		// This adds an inner timeout to ensure Docker gets proper termination signal
		"--stop-timeout", "10",
		// Add unique container name based on run ID to make it easier to track and kill if needed
		"--name", containerName,
		// Image to run
		message.ImageName,
	}

	// Parse and add the command
	cmdName, cmdArgs, err := message.FormCommand()
	if err != nil {
		return &RunResult{
			Output:   "",
			Error:    fmt.Sprintf("failed to parse command: %v", err),
			ExitCode: 1,
			Status:   models.RunStatusFailed,
		}, err
	}

	// Add the command and its arguments
	args = append(args, cmdName)
	args = append(args, cmdArgs...)

	// Create the command with context for timeout handling
	cmd := exec.CommandContext(ctx, "docker", args...)

	// Capture stdout and stderr
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Execute the docker command
	err = cmd.Run()
	cmd.WaitDelay = 1 * time.Second

	result := &RunResult{
		Output:   stdout.String(),
		Error:    stderr.String(),
		ExitCode: 0,
		Status:   models.RunStatusCompleted,
	}

	// Handle various error cases
	if err != nil {
		result.ExitCode = ExitCodeUnknown
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			// Try to force kill the container if timeout occurs
			killCmd := exec.Command("docker", "kill", containerName)
			if killErr := killCmd.Run(); killErr != nil {
				log.Warn().Err(killErr).Str("container", containerName).Msg("Failed to kill container after timeout")
			}

			err = fmt.Errorf("docker command timed out: %w", err)
			result.ExitCode = ExitCodeTimeOut
			result.Status = models.RunStatusCancelled

		case errors.Is(ctx.Err(), context.Canceled):
			// Try to stop the container gracefully if canceled
			stopCmd := exec.Command("docker", "stop", containerName)
			if stopErr := stopCmd.Run(); stopErr != nil {
				log.Warn().Err(stopErr).Str("container", containerName).Msg("Failed to stop container after cancellation")
			}

			err = fmt.Errorf("docker command was canceled: %w", err)
			result.ExitCode = ExitCodeCancelled
			result.Status = models.RunStatusCancelled

		default:
			var exitError *exec.ExitError
			result.Status = models.RunStatusFailed
			if errors.As(err, &exitError) {
				result.ExitCode = exitError.ExitCode()
			}
		}
	}

	return result, err
}

func (w *Worker) RunShellTask(ctx context.Context, message queue.TaskMessage) (*RunResult, error) {
	log.Info().
		Str("type", "shell").
		Int64("run_id", message.RunID).
		Int64("task_id", message.TaskID).
		Str("command", message.Command).
		Msg("Executing task")

	cmdName, args, err := message.FormCommand()
	if err != nil {
		return &RunResult{
			Output:   "",
			Error:    err.Error(),
			ExitCode: 1,
			Status:   models.RunStatusFailed,
		}, err
	}

	cmd := exec.CommandContext(ctx, cmdName, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()

	result := &RunResult{
		Output:   stdout.String(),
		Error:    stderr.String(),
		ExitCode: 0,
		Status:   models.RunStatusCompleted,
	}
	if err != nil {
		result.ExitCode = ExitCodeUnknown
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			err = fmt.Errorf("command timed out: %w", err)
			result.ExitCode = ExitCodeTimeOut
			result.Status = models.RunStatusCancelled
		case errors.Is(ctx.Err(), context.Canceled):
			err = fmt.Errorf("command was canceled: %w", err)
			result.ExitCode = ExitCodeCancelled
			result.Status = models.RunStatusCancelled
		default:
			var exitError *exec.ExitError
			result.Status = models.RunStatusFailed
			if errors.As(err, &exitError) {
				result.ExitCode = exitError.ExitCode()
			}
		}
	}

	return result, err
}

func (w *Worker) Stop() {
	w.cancel()
}
