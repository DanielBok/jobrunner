package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/guregu/null/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"jobrunner/internal/scheduler"
)

func TestDependencyResolver_CheckDependencies(t *testing.T) {
	// Setup test DB
	clearTestDB(t)

	// Create test resolver
	resolver := scheduler.NewDependencyProbe(db)

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
		insertExecution(t, parentJobID, "completed", 0, startTime, endTime, false)

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
		insertExecution(t, parentJobID, "completed", 0, startTime, endTime, true)

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
		insertExecution(t, parentJobID, "failed", 1, startTime, endTime, true)

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
		insertExecution(t, parentJobID, "failed", 1, startTime, endTime, true)

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
		insertExecution(t, parentJobID, "failed", 1, startTime, endTime, true)

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
		insertExecution(t, parentJobID1, "completed", 0, startTime, endTime, true)
		insertExecution(t, parentJobID2, "completed", 0, startTime, endTime, true)

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
		insertExecution(t, parentJobID1, "completed", 0, startTime, endTime, true)
		insertExecution(t, parentJobID2, "failed", 1, startTime, endTime, true)

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
}
