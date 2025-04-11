package worker_test

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"jobrunner/internal/models"
	"jobrunner/internal/queue"
	"jobrunner/internal/worker"
)

// TestIntegrationRunShellTask performs an integration test with real commands
func TestIntegrationRunShellTask(t *testing.T) {
	// Create a worker
	wkr := worker.New(nil, nil)

	t.Run("successful command", func(t *testing.T) {
		// Create a message for a simple echo command
		message := queue.TaskMessage{
			RunID:      123,
			TaskID:     456,
			Command:    "go version",
			Timeout:    60,
			MaxRetries: 3,
		}

		// Execute the task
		result, err := wkr.RunShellTask(context.Background(), message)

		// Verify results
		assert.NoError(t, err)
		assert.Equal(t, "", result.Error)
		assert.Equal(t, runtime.Version(), strings.Split(result.Output, " ")[2])
		assert.Equal(t, 0, result.ExitCode)
		assert.Equal(t, models.RunStatusCompleted, result.Status)
	})

	t.Run("failing command", func(t *testing.T) {
		// Test with a failing command
		message := queue.TaskMessage{
			RunID:      124,
			TaskID:     457,
			Command:    "go bad-cmd",
			Timeout:    60,
			MaxRetries: 3,
		}

		result, err := wkr.RunShellTask(context.Background(), message)

		// Verify failure results
		assert.Error(t, err)
		assert.Equal(t, "", result.Output)
		assert.Equal(t, "go bad-cmd: unknown command\nRun 'go help' for usage.\n", result.Error)
		assert.NotEqual(t, 0, result.ExitCode)
		assert.Equal(t, models.RunStatusFailed, result.Status)
	})

	t.Run("timeout command", func(t *testing.T) {
		var cmd string
		switch runtime.GOOS {
		case "windows":
			cmd = `powershell -Command Start-Sleep 10`
		case "linux":
			cmd = "sh -c sleep 10"
		default:
			t.Skip(fmt.Sprintf("Unsupported OS: %s", runtime.GOOS))
		}

		// Test timeout
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		message := queue.TaskMessage{
			RunID:      125,
			TaskID:     458,
			Command:    cmd,
			Timeout:    60,
			MaxRetries: 3,
		}

		result, err := wkr.RunShellTask(ctx, message)

		// Verify timeout results
		assert.Error(t, err)
		assert.Equal(t, "", result.Output)
		assert.Equal(t, "", result.Error)
		assert.Equal(t, worker.ExitCodeTimeOut, result.ExitCode)
		assert.Equal(t, models.RunStatusCancelled, result.Status)
	})
}

// TestIntegrationRunDockerTask performs an integration test with real docker commands
func TestIntegrationRunDockerTask(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Skip if docker is not available
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker not available, skipping integration test")
	}

	// Create a worker
	wkr := worker.New(nil, nil)

	t.Run("successful command", func(t *testing.T) {
		// Create a message for a simple docker command
		message := queue.TaskMessage{
			RunID:      126,
			TaskID:     459,
			Command:    "echo 'docker integration test'",
			ImageName:  "alpine:latest",
			Timeout:    60,
			MaxRetries: 3,
		}

		// Execute the task
		result, err := wkr.RunDockerTask(context.Background(), message)

		// Verify results
		assert.NoError(t, err)
		assert.Equal(t, strings.TrimSpace(result.Output), "docker integration test")
		assert.Equal(t, "", result.Error)
		assert.Equal(t, 0, result.ExitCode)
		assert.Equal(t, models.RunStatusCompleted, result.Status)
	})

	t.Run("failing command", func(t *testing.T) {
		// Test with a failing command in docker
		message := queue.TaskMessage{
			RunID:      127,
			TaskID:     460,
			Command:    "ls /nonexistentdirectory",
			ImageName:  "alpine:latest",
			Timeout:    60,
			MaxRetries: 3,
		}

		result, err := wkr.RunDockerTask(context.Background(), message)

		// Verify failure results
		assert.Error(t, err)
		assert.Equal(t, "", result.Output)
		assert.Contains(t, result.Error, "No such file or directory")
		assert.NotEqual(t, 0, result.ExitCode)
		assert.Equal(t, models.RunStatusFailed, result.Status)
	})

	t.Run("timeout command", func(t *testing.T) {
		// Test timeout
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		message := queue.TaskMessage{
			RunID:      128,
			TaskID:     461,
			Command:    "sleep 10",
			ImageName:  "alpine:latest",
			Timeout:    60,
			MaxRetries: 3,
		}

		result, err := wkr.RunDockerTask(ctx, message)

		// Verify timeout results
		assert.Error(t, err)
		assert.Equal(t, "", result.Output)
		assert.Equal(t, "", result.Error)
		assert.Equal(t, worker.ExitCodeTimeOut, result.ExitCode)
		assert.Equal(t, models.RunStatusCancelled, result.Status)
	})
}
