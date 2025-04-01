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
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.JobDependency{})
		require.NoError(t, err)
		assert.True(t, met)
	})

	t.Run("successful dependency with valid end time", func(t *testing.T) {
		// Insert test data
		jobID := insertJob(t, "Test Job", "Echo Hello World")
		parentJobID := insertJob(t, "Parent Job", "Echo Hello World")

		// Insert dependency
		depID := insertDependency(t, jobID, parentJobID, "success", 3600, 0)

		// Insert successful execution that ended recently
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentJobID, "completed", 0, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.JobDependency{{
			ID:                depID,
			JobID:             jobID,
			DependsOn:         parentJobID,
			LookbackWindow:    3600,
			RequiredCondition: "success",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.True(t, met)
	})

	t.Run("expired dependency outside lookback window", func(t *testing.T) {
		// Insert test data
		jobID := insertJob(t, "Test Job 2", "echo Hello World")
		parentJobID := insertJob(t, "Parent Job 2", "echo Hello World")

		// Insert dependency with short lookback window (10 seconds)
		depID := insertDependency(t, jobID, parentJobID, "success", 10, 0)

		// Insert successful execution that ended too long ago (15 minutes)
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-15 * time.Minute))
		insertExecution(t, parentJobID, "completed", 0, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.JobDependency{{
			ID:                depID,
			JobID:             jobID,
			DependsOn:         parentJobID,
			LookbackWindow:    10,
			RequiredCondition: "success",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.False(t, met, "Should be false because execution is outside lookback window")
	})

	t.Run("dependency with null end time", func(t *testing.T) {
		// Insert test data
		jobID := insertJob(t, "Test Job 3", "Echo Hello World")
		parentJobID := insertJob(t, "Parent Job 3", "Echo Hello World")

		// Insert dependency
		depID := insertDependency(t, jobID, parentJobID, "success", 3600, 0)

		// Insert execution with null end time (still running)
		insertTestExecutionWithNullEndTime(t, db, parentJobID, "running", -1)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.JobDependency{{
			ID:                depID,
			JobID:             jobID,
			DependsOn:         parentJobID,
			LookbackWindow:    3600,
			RequiredCondition: "success",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.False(t, met, "Should be false with null end time")
	})

	t.Run("dependency with wrong exit code for success condition", func(t *testing.T) {
		// Insert test data
		jobID := insertJob(t, "Test Job 4", "echo Hello World")
		parentJobID := insertJob(t, "Parent Job 4", "echo Hello World")

		// Insert dependency requiring success
		depID := insertDependency(t, jobID, parentJobID, "success", 3600, 0)

		// Insert failed execution (non-zero exit code)
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-15 * time.Minute))
		insertExecution(t, parentJobID, "failed", 1, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.JobDependency{{
			ID:                depID,
			JobID:             jobID,
			DependsOn:         parentJobID,
			LookbackWindow:    3600,
			RequiredCondition: "success",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.False(t, met, "Should be false because exit code is not 0")
	})

	t.Run("dependency with completion condition", func(t *testing.T) {
		// Insert test data
		jobID := insertJob(t, "Test Job 5", "echo Hello World")
		parentJobID := insertJob(t, "Parent Job 5", "echo Hello World")

		// Insert dependency requiring only completion
		depID := insertDependency(t, jobID, parentJobID, "completion", 3600, 0)

		// Insert failed execution with proper exit code (not -1)
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentJobID, "failed", 1, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.JobDependency{{
			ID:                depID,
			JobID:             jobID,
			DependsOn:         parentJobID,
			LookbackWindow:    3600,
			RequiredCondition: "completion",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.True(t, met, "Should be true for completion condition with any non -1 exit code")
	})

	t.Run("dependency with failure condition", func(t *testing.T) {
		// Insert test data
		jobID := insertJob(t, "Test Job 6", "echo Hello World")
		parentJobID := insertJob(t, "Parent Job 6", "echo Hello World")

		// Insert dependency requiring failure
		depID := insertDependency(t, jobID, parentJobID, "failure", 3600, 0)

		// Insert failed execution
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentJobID, "failed", 1, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.JobDependency{{
			ID:                depID,
			JobID:             jobID,
			DependsOn:         parentJobID,
			LookbackWindow:    3600,
			RequiredCondition: "failure",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.True(t, met, "Should be true for failure condition with positive exit code")
	})

	t.Run("multiple dependencies all met", func(t *testing.T) {
		// Insert test data
		jobID := insertJob(t, "Test Job 7", "echo Hello World")
		parentJobID1 := insertJob(t, "Parent Job 7-1", "echo Hello World")
		parentJobID2 := insertJob(t, "Parent Job 7-2", "echo Hello World")

		lookbackWindow := 3600
		// Insert dependencies
		depID1 := insertDependency(t, jobID, parentJobID1, "success", lookbackWindow, 0)
		depID2 := insertDependency(t, jobID, parentJobID2, "success", lookbackWindow, 0)

		// Insert successful executions
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentJobID1, "completed", 0, startTime, endTime)
		insertExecution(t, parentJobID2, "completed", 0, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.JobDependency{
			{
				ID:                depID1,
				JobID:             jobID,
				DependsOn:         parentJobID1,
				LookbackWindow:    lookbackWindow,
				RequiredCondition: "success",
				MinWaitSeconds:    0,
			}, {
				ID:                depID2,
				JobID:             jobID,
				DependsOn:         parentJobID2,
				LookbackWindow:    lookbackWindow,
				RequiredCondition: "success",
				MinWaitSeconds:    0,
			}})
		require.NoError(t, err)
		assert.True(t, met, "Should be true when all dependencies are met")
	})

	t.Run("multiple dependencies one not met", func(t *testing.T) {
		// Insert test data
		jobID := insertJob(t, "Test Job 8", "echo Hello World")
		parentJobID1 := insertJob(t, "Parent Job 8-1", "echo Hello World")
		parentJobID2 := insertJob(t, "Parent Job 8-2", "echo Hello World")

		lookbackWindow := 3600
		// Insert dependencies
		depID1 := insertDependency(t, jobID, parentJobID1, "success", lookbackWindow, 0)
		depID2 := insertDependency(t, jobID, parentJobID2, "success", lookbackWindow, 0)

		// Insert one successful and one failed execution
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentJobID1, "completed", 0, startTime, endTime)
		insertExecution(t, parentJobID2, "failed", 1, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.JobDependency{
			{
				ID:                depID1,
				JobID:             jobID,
				DependsOn:         parentJobID1,
				LookbackWindow:    lookbackWindow,
				RequiredCondition: "success",
				MinWaitSeconds:    0,
			}, {
				ID:                depID2,
				JobID:             jobID,
				DependsOn:         parentJobID2,
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
		jobID := insertJob(t, "Test Job Cancelled", "echo Hello World")
		parentJobID := insertJob(t, "Parent Job Cancelled", "echo Hello World")

		// Insert dependency requiring cancellation
		depID := insertDependency(t, jobID, parentJobID, "cancelled", 3600, 0)

		// Insert cancelled execution
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentJobID, "cancelled", -1, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.JobDependency{{
			ID:                depID,
			JobID:             jobID,
			DependsOn:         parentJobID,
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
		jobID := insertJob(t, "Test Job Lapsed", "echo Hello World")
		parentJobID := insertJob(t, "Parent Job Lapsed", "echo Hello World")

		// Insert dependency requiring lapsed
		depID := insertDependency(t, jobID, parentJobID, "lapsed", 3600, 0)

		// Insert lapsed execution
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentJobID, "lapsed", -1, startTime, endTime)

		// Check dependencies
		met, err := resolver.CheckDependencies(context.Background(), []scheduler.JobDependency{{
			ID:                depID,
			JobID:             jobID,
			DependsOn:         parentJobID,
			LookbackWindow:    3600,
			RequiredCondition: "lapsed",
			MinWaitSeconds:    0,
		}})
		require.NoError(t, err)
		assert.True(t, met, "Should be true for lapsed condition with lapsed status")
	})
}

func TestDependencyProbe_ProcessPendingJob(t *testing.T) {
	clearTestDB(t)

	// Create a mock queue client
	mockQueue := new(MockQueueClient)

	// Create a test DependencyProbe with the mock queue
	probe := scheduler.NewDependencyProbe(db, mockQueue)

	t.Run("job with dependencies all met should publish to queue", func(t *testing.T) {
		// Insert test data
		jobID := insertJob(t, "Process Job 1", "echo Processing")
		parentJobID := insertJob(t, "Parent Job 1", "echo Parent")

		// Insert dependency
		insertDependency(t, jobID, parentJobID, "success", 3600, 0)

		// Insert successful parent execution
		startTime := null.NewTime(time.Now(), false)
		endTime := null.TimeFrom(time.Now().Add(-time.Minute))
		insertExecution(t, parentJobID, "completed", 0, startTime, endTime)

		// Insert pending execution
		execID := insertExecution(t, jobID, "pending", -1, null.NewTime(time.Time{}, false), null.NewTime(time.Time{}, false))

		// Setup the mock to expect a publish call
		mockQueue.On("Publish", mock.Anything, mock.MatchedBy(func(msg queue.TaskMessage) bool {
			return msg.ExecutionID == execID && msg.JobID == jobID
		})).Return(nil).Once()

		// Fetch the pending job
		pendingJobs, err := probe.FetchPendingJobs(context.Background())
		require.NoError(t, err)
		require.NotEmpty(t, pendingJobs, "Should find at least one pending job")

		// Find our specific job
		var ourJob scheduler.PendingJob
		for _, job := range pendingJobs {
			if job.ID == execID {
				ourJob = job
				break
			}
		}
		require.NotZero(t, ourJob.ID, "Should find our specific pending job")

		// Process the pending job
		err = probe.ProcessPendingJob(context.Background(), ourJob)
		require.NoError(t, err)

		// Verify mock expectations
		mockQueue.AssertExpectations(t)
	})

	// This test can remain largely the same, but with a better structure
	t.Run("job past cutoff time should be marked as lapsed", func(t *testing.T) {
		// Insert test data
		jobID := insertJob(t, "Process Job 2", "echo Processing")
		parentJobID := insertJob(t, "Parent Job 2", "echo Parent")

		// Insert dependency with very short lookback window
		insertDependency(t, jobID, parentJobID, "success", 1, 0) // 1 second lookback

		// No parent execution (to ensure dependency isn't met)

		// Insert pending execution that's older than the lookback window
		createdTime := time.Now().Add(-10 * time.Second) // Well past the 1 second lookback

		var execID int64
		err := db.QueryRow(`
			INSERT INTO tasks.execution (job_id, status, created_at)
			VALUES ($1, $2, $3)
			RETURNING id
		`, jobID, models.EsPending, createdTime).Scan(&execID)
		require.NoError(t, err)

		// Fetch the pending job
		pendingJobs, err := probe.FetchPendingJobs(context.Background())
		require.NoError(t, err)

		// Find our specific job
		var ourJob scheduler.PendingJob
		for _, job := range pendingJobs {
			if job.ID == execID {
				ourJob = job
				break
			}
		}
		require.NotZero(t, ourJob.ID, "Should find our specific pending job")

		// Process the pending job
		err = probe.ProcessPendingJob(context.Background(), ourJob)
		require.Error(t, err, "Should return error because job is lapsed")

		// Verify the execution status was updated to lapsed
		var status string
		err = db.QueryRow("SELECT status FROM tasks.execution WHERE id = $1", execID).Scan(&status)
		require.NoError(t, err)
		assert.Equal(t, string(models.EsLapsed), status, "Execution should be marked as lapsed")
	})
}

func TestDependencyProbe_FetchPendingJobs(t *testing.T) {
	clearTestDB(t)

	// Create some test data with pending jobs
	jobID1 := insertJob(t, "Pending Job 1", "echo Pending 1")
	jobID2 := insertJob(t, "Pending Job 2", "echo Pending 2")

	// Insert pending executions
	execID1 := insertExecution(t, jobID1, "pending", -1, null.NewTime(time.Time{}, false), null.NewTime(time.Time{}, false))
	execID2 := insertExecution(t, jobID2, "pending", -1, null.NewTime(time.Time{}, false), null.NewTime(time.Time{}, false))

	// Create the probe
	probe := scheduler.NewDependencyProbe(db, rq)

	// Test fetching pending jobs
	pendingJobs, err := probe.FetchPendingJobs(context.Background())
	require.NoError(t, err)

	// Check that we got our pending jobs
	foundJob1 := false
	foundJob2 := false

	for _, job := range pendingJobs {
		if job.ID == execID1 && job.JobID == jobID1 {
			foundJob1 = true
		}
		if job.ID == execID2 && job.JobID == jobID2 {
			foundJob2 = true
		}
	}

	assert.True(t, foundJob1, "Should find first pending job")
	assert.True(t, foundJob2, "Should find second pending job")
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
