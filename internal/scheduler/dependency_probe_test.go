package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/guregu/null/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"jobrunner/internal/models"
	"jobrunner/internal/queue"
	"jobrunner/internal/scheduler"
)

func TestDependencyResolver_CheckDependencies(t *testing.T) {
	// Setup test DB
	clearTestDB(t)

	// Create test resolver
	resolver := scheduler.NewDependencyProbe(db, rq)

	// Test cases
	t.Run("no dependencies should return true", func(t *testing.T) {
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.TaskDependency{})
		require.NoError(t, err)
		assert.True(t, met)
	})

	t.Run("successful dependency with valid end time", func(t *testing.T) {
		// Insert test data
		taskID := insertTask(t, "Test Task", "Echo Hello World")
		parentTaskID := insertTask(t, "Parent Task", "Echo Hello World")

		// Insert dependency
		depID := insertDependency(t, taskID, parentTaskID, "success", 3600, 0)

		// Insert successful execution that ended recently
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentTaskID, "completed", 0, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.TaskDependency{{
			ID:                depID,
			TaskID:            taskID,
			DependsOn:         parentTaskID,
			LookbackWindow:    3600,
			RequiredCondition: "success",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.True(t, met)
	})

	t.Run("expired dependency outside lookback window", func(t *testing.T) {
		// Insert test data
		taskID := insertTask(t, "Test Task 2", "echo Hello World")
		parentTaskID := insertTask(t, "Parent Task 2", "echo Hello World")

		// Insert dependency with short lookback window (10 seconds)
		depID := insertDependency(t, taskID, parentTaskID, "success", 10, 0)

		// Insert successful execution that ended too long ago (15 minutes)
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-15 * time.Minute))
		insertExecution(t, parentTaskID, "completed", 0, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.TaskDependency{{
			ID:                depID,
			TaskID:            taskID,
			DependsOn:         parentTaskID,
			LookbackWindow:    10,
			RequiredCondition: "success",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.False(t, met, "Should be false because execution is outside lookback window")
	})

	t.Run("dependency with null end time", func(t *testing.T) {
		// Insert test data
		taskID := insertTask(t, "Test Task 3", "Echo Hello World")
		parentTaskID := insertTask(t, "Parent Task 3", "Echo Hello World")

		// Insert dependency
		depID := insertDependency(t, taskID, parentTaskID, "success", 3600, 0)

		// Insert execution with null end time (still running)
		insertTestExecutionWithNullEndTime(t, db, parentTaskID, "running", -1)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.TaskDependency{{
			ID:                depID,
			TaskID:            taskID,
			DependsOn:         parentTaskID,
			LookbackWindow:    3600,
			RequiredCondition: "success",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.False(t, met, "Should be false with null end time")
	})

	t.Run("dependency with wrong exit code for success condition", func(t *testing.T) {
		// Insert test data
		taskID := insertTask(t, "Test Task 4", "echo Hello World")
		parentTaskID := insertTask(t, "Parent Task 4", "echo Hello World")

		// Insert dependency requiring success
		depID := insertDependency(t, taskID, parentTaskID, "success", 3600, 0)

		// Insert failed execution (non-zero exit code)
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-15 * time.Minute))
		insertExecution(t, parentTaskID, "failed", 1, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.TaskDependency{{
			ID:                depID,
			TaskID:            taskID,
			DependsOn:         parentTaskID,
			LookbackWindow:    3600,
			RequiredCondition: "success",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.False(t, met, "Should be false because exit code is not 0")
	})

	t.Run("dependency with completion condition", func(t *testing.T) {
		// Insert test data
		taskID := insertTask(t, "Test Task 5", "echo Hello World")
		parentTaskID := insertTask(t, "Parent Task 5", "echo Hello World")

		// Insert dependency requiring only completion
		depID := insertDependency(t, taskID, parentTaskID, "completion", 3600, 0)

		// Insert failed execution with proper exit code (not -1)
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentTaskID, "failed", 1, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.TaskDependency{{
			ID:                depID,
			TaskID:            taskID,
			DependsOn:         parentTaskID,
			LookbackWindow:    3600,
			RequiredCondition: "completion",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.True(t, met, "Should be true for completion condition with any non -1 exit code")
	})

	t.Run("dependency with failure condition", func(t *testing.T) {
		// Insert test data
		taskID := insertTask(t, "Test Task 6", "echo Hello World")
		parentTaskID := insertTask(t, "Parent Task 6", "echo Hello World")

		// Insert dependency requiring failure
		depID := insertDependency(t, taskID, parentTaskID, "failure", 3600, 0)

		// Insert failed execution
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentTaskID, "failed", 1, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.TaskDependency{{
			ID:                depID,
			TaskID:            taskID,
			DependsOn:         parentTaskID,
			LookbackWindow:    3600,
			RequiredCondition: "failure",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.True(t, met, "Should be true for failure condition with positive exit code")
	})

	t.Run("multiple dependencies all met", func(t *testing.T) {
		// Insert test data
		taskID := insertTask(t, "Test Task 7", "echo Hello World")
		parentTaskID1 := insertTask(t, "Parent Task 7-1", "echo Hello World")
		parentTaskID2 := insertTask(t, "Parent Task 7-2", "echo Hello World")

		lookbackWindow := 3600
		// Insert dependencies
		depID1 := insertDependency(t, taskID, parentTaskID1, "success", lookbackWindow, 0)
		depID2 := insertDependency(t, taskID, parentTaskID2, "success", lookbackWindow, 0)

		// Insert successful executions
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentTaskID1, "completed", 0, startTime, endTime)
		insertExecution(t, parentTaskID2, "completed", 0, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.TaskDependency{
			{
				ID:                depID1,
				TaskID:            taskID,
				DependsOn:         parentTaskID1,
				LookbackWindow:    lookbackWindow,
				RequiredCondition: "success",
				MinWaitSeconds:    0,
			}, {
				ID:                depID2,
				TaskID:            taskID,
				DependsOn:         parentTaskID2,
				LookbackWindow:    lookbackWindow,
				RequiredCondition: "success",
				MinWaitSeconds:    0,
			}})
		require.NoError(t, err)
		assert.True(t, met, "Should be true when all dependencies are met")
	})

	t.Run("multiple dependencies one not met", func(t *testing.T) {
		// Insert test data
		taskID := insertTask(t, "Test Task 8", "echo Hello World")
		parentTaskID1 := insertTask(t, "Parent Task 8-1", "echo Hello World")
		parentTaskID2 := insertTask(t, "Parent Task 8-2", "echo Hello World")

		lookbackWindow := 3600
		// Insert dependencies
		depID1 := insertDependency(t, taskID, parentTaskID1, "success", lookbackWindow, 0)
		depID2 := insertDependency(t, taskID, parentTaskID2, "success", lookbackWindow, 0)

		// Insert one successful and one failed execution
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentTaskID1, "completed", 0, startTime, endTime)
		insertExecution(t, parentTaskID2, "failed", 1, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.TaskDependency{
			{
				ID:                depID1,
				TaskID:            taskID,
				DependsOn:         parentTaskID1,
				LookbackWindow:    lookbackWindow,
				RequiredCondition: "success",
				MinWaitSeconds:    0,
			}, {
				ID:                depID2,
				TaskID:            taskID,
				DependsOn:         parentTaskID2,
				LookbackWindow:    lookbackWindow,
				RequiredCondition: "success",
				MinWaitSeconds:    0,
			}})
		require.NoError(t, err)
		assert.False(t, met, "Should be false when any dependency is not met")
	})

	// Add test for cancelled condition
	t.Run("dependency with cancelled condition", func(t *testing.T) {
		// Insert test data
		taskID := insertTask(t, "Test Task Cancelled", "echo Hello World")
		parentTaskID := insertTask(t, "Parent Task Cancelled", "echo Hello World")

		// Insert dependency requiring cancellation
		depID := insertDependency(t, taskID, parentTaskID, "cancelled", 3600, 0)

		// Insert cancelled execution
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentTaskID, "cancelled", -1, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.TaskDependency{{
			ID:                depID,
			TaskID:            taskID,
			DependsOn:         parentTaskID,
			LookbackWindow:    3600,
			RequiredCondition: "cancelled",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.True(t, met, "Should be true for cancelled condition with cancelled status")
	})

	// Add test for lapsed condition
	t.Run("dependency with lapsed condition", func(t *testing.T) {
		// Insert test data
		taskID := insertTask(t, "Test Task Lapsed", "echo Hello World")
		parentTaskID := insertTask(t, "Parent Task Lapsed", "echo Hello World")

		// Insert dependency requiring lapsed
		depID := insertDependency(t, taskID, parentTaskID, "lapsed", 3600, 0)

		// Insert lapsed execution
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentTaskID, "lapsed", -1, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.TaskDependency{{
			ID:                depID,
			TaskID:            taskID,
			DependsOn:         parentTaskID,
			LookbackWindow:    3600,
			RequiredCondition: "lapsed",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.True(t, met, "Should be true for lapsed condition with lapsed status")
	})
}

func TestDependencyProbe_ProcessPendingTask(t *testing.T) {
	clearTestDB(t)

	// Create a mock queue client
	mockQueue := new(MockQueueClient)

	// Create a test DependencyProbe with the mock queue
	probe := scheduler.NewDependencyProbe(db, mockQueue)

	t.Run("task with dependencies all met should publish to queue", func(t *testing.T) {
		// Insert test data
		taskID := insertTask(t, "Process Task 1", "echo Processing")
		parentTaskID := insertTask(t, "Parent Task 1", "echo Parent")

		// Insert dependency
		insertDependency(t, taskID, parentTaskID, "success", 3600, 0)

		// Insert successful parent execution
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentTaskID, "completed", 0, startTime, endTime)

		// Insert pending execution
		execID := insertExecution(t, taskID, "pending", -1, null.NewTime(time.Time{}, false), null.NewTime(time.Time{}, false))

		// Setup the mock to expect a publish call
		mockQueue.On("Publish", mock.Anything, mock.MatchedBy(func(msg queue.TaskMessage) bool {
			return msg.RunID == execID && msg.TaskID == taskID
		})).Return(nil).Once()

		// Fetch the pending task
		pendingTasks, err := probe.FetchPendingTasks(context.Background())
		require.NoError(t, err)
		require.NotEmpty(t, pendingTasks, "Should find at least one pending task")

		// Find our specific task
		var ourTask scheduler.PendingTask
		for _, task := range pendingTasks {
			if task.ID == execID {
				ourTask = task
				break
			}
		}
		require.NotZero(t, ourTask.ID, "Should find our specific pending task")

		// Process the pending task
		err = probe.ProcessPendingTask(context.Background(), ourTask)
		require.NoError(t, err)

		// Verify mock expectations
		mockQueue.AssertExpectations(t)
	})

	// This test can remain largely the same, but with a better structure
	t.Run("task past cutoff time should be marked as lapsed", func(t *testing.T) {
		// Insert test data
		taskID := insertTask(t, "Process Task 2", "echo Processing")
		parentTaskID := insertTask(t, "Parent Task 2", "echo Parent")

		// Insert dependency with very short lookback window
		insertDependency(t, taskID, parentTaskID, "success", 1, 0) // 1 second lookback

		// No parent execution (to ensure dependency isn't met)

		// Insert pending execution that's older than the lookback window
		createdTime := time.Now().Add(-10 * time.Second) // Well past the 1 second lookback

		var execID int64
		err := db.QueryRow(`
			INSERT INTO task.run (task_id, status, created_at)
			VALUES ($1, $2, $3)
			RETURNING id
		`, taskID, models.RunStatusPending, createdTime).Scan(&execID)
		require.NoError(t, err)

		// Fetch the pending task
		pendingTasks, err := probe.FetchPendingTasks(context.Background())
		require.NoError(t, err)

		// Find our specific task
		var ourTask scheduler.PendingTask
		for _, task := range pendingTasks {
			if task.ID == execID {
				ourTask = task
				break
			}
		}
		require.NotZero(t, ourTask.ID, "Should find our specific pending task")

		// Process the pending task
		err = probe.ProcessPendingTask(context.Background(), ourTask)
		require.Error(t, err, "Should return error because task is lapsed")

		// Verify the execution status was updated to lapsed
		var status string
		err = db.QueryRow("SELECT status FROM task.run WHERE id = $1", execID).Scan(&status)
		require.NoError(t, err)
		assert.Equal(t, string(models.RunStatusLapsed), status, "Execution should be marked as lapsed")
	})
}

func TestDependencyProbe_FetchPendingTasks(t *testing.T) {
	clearTestDB(t)

	// Create some test data with pending tasks
	taskID1 := insertTask(t, "Pending Task 1", "echo Pending 1")
	taskID2 := insertTask(t, "Pending Task 2", "echo Pending 2")

	// Insert pending executions
	execID1 := insertExecution(t, taskID1, "pending", -1, null.NewTime(time.Time{}, false), null.NewTime(time.Time{}, false))
	execID2 := insertExecution(t, taskID2, "pending", -1, null.NewTime(time.Time{}, false), null.NewTime(time.Time{}, false))

	// Create the probe
	probe := scheduler.NewDependencyProbe(db, rq)

	// Test fetching pending tasks
	pendingTasks, err := probe.FetchPendingTasks(context.Background())
	require.NoError(t, err)

	// Check that we got our pending tasks
	foundTask1 := false
	foundTask2 := false

	for _, task := range pendingTasks {
		if task.ID == execID1 && task.TaskID == taskID1 {
			foundTask1 = true
		}
		if task.ID == execID2 && task.TaskID == taskID2 {
			foundTask2 = true
		}
	}

	assert.True(t, foundTask1, "Should find first pending task")
	assert.True(t, foundTask2, "Should find second pending task")
}

func TestDependencyProbe_StartStop(t *testing.T) {
	clearTestDB(t)

	probe := scheduler.NewDependencyProbe(db, rq)
	ctx := context.Background()

	// Start the probe
	probe.Start(ctx)

	// Wait a moment to ensure it's running
	time.Sleep(50 * time.Millisecond)

	// Stop the probe
	probe.Stop()

	// Ensure it can be started again
	probe.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	probe.Stop()

	// Start with a cancelled context
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	probe.Start(cancelCtx)
	time.Sleep(50 * time.Millisecond)

	// Redundant calls should not cause issues
	probe.Stop()
	probe.Stop()
}
