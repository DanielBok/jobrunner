package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"jobrunner/internal/scheduler"
)

func TestGetAllSchedules(t *testing.T) {
	// Create a new mock database
	clearTestDB(t)
	taskScheduler := scheduler.NewTaskScheduler(db, nil)
	idMap := preloadTestDB(t)

	// Call the function
	tasks, err := taskScheduler.GetAllSchedules(context.Background())
	assert.NoError(t, err)
	assert.Len(t, tasks, 6)

	//Verify first task
	assert.Equal(t, tasks[0].TaskName, "Task 1")
	assert.Len(t, tasks[0].Dependencies, 0)

	// Verify second Task
	assert.Equal(t, tasks[1].TaskName, "Task 2")
	assert.Len(t, tasks[1].Dependencies, 1)
	assert.ElementsMatch(t, []int64{idMap[1]}, tasks[1].ParentIDs(), "Task 2 should depend on Task 1")

	// Verify third Task. Third Task has 4 schedules
	for i := 2; i < len(tasks); i++ {
		task := tasks[i]
		assert.Equal(t, task.TaskName, "Task 3")
		assert.Len(t, task.Dependencies, 2)

		assert.ElementsMatch(t, []int64{idMap[1], idMap[2]}, task.ParentIDs(), "Task 3 should depend on Tasks 1 and 2")
	}
}

// Create the task and schedule
func TestTimezoneScheduling(t *testing.T) {
	c := cron.New(cron.WithLocation(time.UTC))
	c.Start()
	defer c.Stop()

	// Add entries (but don't start cron)
	sgEntry, _ := c.AddFunc("CRON_TZ=Asia/Singapore 30 19 * * *", func() {})
	chiEntry, _ := c.AddFunc("CRON_TZ=America/Chicago 30 19 * * *", func() {})

	// Get next execution times (from the current time)
	sgNext := c.Entry(sgEntry).Next
	chiNext := c.Entry(chiEntry).Next

	assert.Equal(t, sgNext.Minute(), 30)
	assert.Equal(t, sgNext.Hour(), 11)
	assert.Equal(t, chiNext.Minute(), 30)
	assert.NotEqual(t, chiNext.Hour(), sgNext.Hour())
}

func TestAddAndRemoveSchedule(t *testing.T) {
	// Create a scheduler with a separate cron instance for this test
	clearTestDB(t)
	preloadTestDB(t)

	taskScheduler := scheduler.NewTaskScheduler(db, nil)

	// Ensure we're starting clean
	taskScheduler.RemoveSchedule(1)
	taskScheduler.RemoveSchedule(2)
	taskScheduler.RemoveSchedule(3)

	// Get test data from the database
	ctx := context.Background()
	allSchedules, err := taskScheduler.GetAllSchedules(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, allSchedules, "Need test data to proceed")

	for _, sch := range allSchedules {
		err := taskScheduler.AddSchedule(ctx, sch)
		assert.NoError(t, err, "Should add schedule without error")
	}

	for _, sch := range allSchedules {
		taskScheduler.RemoveSchedule(sch.ScheduleID)
	}
}
