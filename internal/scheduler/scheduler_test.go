package scheduler_test

import (
	"context"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	jrs "jobrunner/internal/scheduler"
)

func TestGetAllSchedules(t *testing.T) {
	// Create a new mock database
	clearTestDB(t)
	scheduler := jrs.NewScheduler(db)
	idMap := preloadTestDB(t)

	// Call the function
	jobs, err := scheduler.GetAllSchedules(context.Background())
	assert.NoError(t, err)
	assert.Len(t, jobs, 6)

	//Verify first job
	assert.Equal(t, jobs[0].JobName, "Job 1")
	assert.Len(t, jobs[0].Dependencies, 0)

	// Verify second Job
	assert.Equal(t, jobs[1].JobName, "Job 2")
	assert.Len(t, jobs[1].Dependencies, 1)
	assert.ElementsMatch(t, []int64{idMap[1]}, jobs[1].ParentIDs(), "Job 2 should depend on Job 1")

	// Verify third Job. Third Job has 4 schedules
	for i := 2; i < len(jobs); i++ {
		job := jobs[i]
		assert.Equal(t, job.JobName, "Job 3")
		assert.Len(t, job.Dependencies, 2)

		assert.ElementsMatch(t, []int64{idMap[1], idMap[2]}, job.ParentIDs(), "Job 3 should depend on Jobs 1 and 2")
	}
}

// Create the job and schedule
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

	scheduler := jrs.NewScheduler(db)

	// Ensure we're starting clean
	scheduler.RemoveSchedule(1)
	scheduler.RemoveSchedule(2)
	scheduler.RemoveSchedule(3)

	// Get test data from the database
	ctx := context.Background()
	allSchedules, err := scheduler.GetAllSchedules(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, allSchedules, "Need test data to proceed")

	for _, sch := range allSchedules {
		err := scheduler.AddSchedule(ctx, sch)
		assert.NoError(t, err, "Should add schedule without error")
	}

	for _, sch := range allSchedules {
		scheduler.RemoveSchedule(sch.ScheduleID)
	}
}
