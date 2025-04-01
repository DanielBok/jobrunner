package scheduler_test

import (
	"testing"

	"github.com/guregu/null/v6"
	"github.com/stretchr/testify/assert"
	"jobrunner/internal/models"
	"jobrunner/internal/scheduler"
)

func TestSchedule_Equal(t *testing.T) {
	// Create base schedule
	baseSchedule := &scheduler.Schedule{
		ScheduleID:     1,
		JobID:          100,
		JobName:        "Test Job",
		ImageName:      null.StringFrom("ubuntu:latest"),
		Command:        "echo hello",
		TimeoutSeconds: 3600,
		MaxRetries:     3,
		CronExpression: "0 0 * * *",
		IsActive:       true,
		Dependencies: []scheduler.JobDependency{
			{
				ID:                1,
				JobID:             100,
				DependsOn:         99,
				LookbackWindow:    3600,
				RequiredCondition: models.RcSuccess,
				MinWaitSeconds:    0,
			},
		},
	}

	t.Run("identical schedules should be equal", func(t *testing.T) {
		// Create a copy of the base schedule
		copySchedule := &scheduler.Schedule{
			ScheduleID:     1,
			JobID:          100,
			JobName:        "Test Job",
			ImageName:      null.StringFrom("ubuntu:latest"),
			Command:        "echo hello",
			TimeoutSeconds: 3600,
			MaxRetries:     3,
			CronExpression: "0 0 * * *",
			IsActive:       true,
			Dependencies: []scheduler.JobDependency{
				{
					ID:                1,
					JobID:             100,
					DependsOn:         99,
					LookbackWindow:    3600,
					RequiredCondition: models.RcSuccess,
					MinWaitSeconds:    0,
				},
			},
		}

		assert.True(t, baseSchedule.Equal(copySchedule))
		assert.True(t, copySchedule.Equal(baseSchedule))
	})

	t.Run("different ScheduleID should not be equal", func(t *testing.T) {
		differentSchedule := &scheduler.Schedule{
			ScheduleID:     2, // Different ScheduleID
			JobID:          100,
			JobName:        "Test Job",
			ImageName:      null.StringFrom("ubuntu:latest"),
			Command:        "echo hello",
			TimeoutSeconds: 3600,
			MaxRetries:     3,
			CronExpression: "0 0 * * *",
			IsActive:       true,
			Dependencies:   baseSchedule.Dependencies,
		}

		assert.False(t, baseSchedule.Equal(differentSchedule))
	})

	t.Run("different JobID should not be equal", func(t *testing.T) {
		differentSchedule := &scheduler.Schedule{
			ScheduleID:     1,
			JobID:          101, // Different JobID
			JobName:        "Test Job",
			ImageName:      null.StringFrom("ubuntu:latest"),
			Command:        "echo hello",
			TimeoutSeconds: 3600,
			MaxRetries:     3,
			CronExpression: "0 0 * * *",
			IsActive:       true,
			Dependencies:   baseSchedule.Dependencies,
		}

		assert.False(t, baseSchedule.Equal(differentSchedule))
	})

	t.Run("different JobName should not be equal", func(t *testing.T) {
		differentSchedule := &scheduler.Schedule{
			ScheduleID:     1,
			JobID:          100,
			JobName:        "Different Job", // Different JobName
			ImageName:      null.StringFrom("ubuntu:latest"),
			Command:        "echo hello",
			TimeoutSeconds: 3600,
			MaxRetries:     3,
			CronExpression: "0 0 * * *",
			IsActive:       true,
			Dependencies:   baseSchedule.Dependencies,
		}

		assert.False(t, baseSchedule.Equal(differentSchedule))
	})

	t.Run("different ImageName should not be equal", func(t *testing.T) {
		differentSchedule := &scheduler.Schedule{
			ScheduleID:     1,
			JobID:          100,
			JobName:        "Test Job",
			ImageName:      null.StringFrom("alpine:latest"), // Different ImageName
			Command:        "echo hello",
			TimeoutSeconds: 3600,
			MaxRetries:     3,
			CronExpression: "0 0 * * *",
			IsActive:       true,
			Dependencies:   baseSchedule.Dependencies,
		}

		assert.False(t, baseSchedule.Equal(differentSchedule))
	})

	t.Run("different Command should not be equal", func(t *testing.T) {
		differentSchedule := &scheduler.Schedule{
			ScheduleID:     1,
			JobID:          100,
			JobName:        "Test Job",
			ImageName:      null.StringFrom("ubuntu:latest"),
			Command:        "echo world", // Different Command
			TimeoutSeconds: 3600,
			MaxRetries:     3,
			CronExpression: "0 0 * * *",
			IsActive:       true,
			Dependencies:   baseSchedule.Dependencies,
		}

		assert.False(t, baseSchedule.Equal(differentSchedule))
	})

	t.Run("different TimeoutSeconds should not be equal", func(t *testing.T) {
		differentSchedule := &scheduler.Schedule{
			ScheduleID:     1,
			JobID:          100,
			JobName:        "Test Job",
			ImageName:      null.StringFrom("ubuntu:latest"),
			Command:        "echo hello",
			TimeoutSeconds: 1800, // Different TimeoutSeconds
			MaxRetries:     3,
			CronExpression: "0 0 * * *",
			IsActive:       true,
			Dependencies:   baseSchedule.Dependencies,
		}

		assert.False(t, baseSchedule.Equal(differentSchedule))
	})

	t.Run("different MaxRetries should not be equal", func(t *testing.T) {
		differentSchedule := &scheduler.Schedule{
			ScheduleID:     1,
			JobID:          100,
			JobName:        "Test Job",
			ImageName:      null.StringFrom("ubuntu:latest"),
			Command:        "echo hello",
			TimeoutSeconds: 3600,
			MaxRetries:     5, // Different MaxRetries
			CronExpression: "0 0 * * *",
			IsActive:       true,
			Dependencies:   baseSchedule.Dependencies,
		}

		assert.False(t, baseSchedule.Equal(differentSchedule))
	})

	t.Run("different CronExpression should not be equal", func(t *testing.T) {
		differentSchedule := &scheduler.Schedule{
			ScheduleID:     1,
			JobID:          100,
			JobName:        "Test Job",
			ImageName:      null.StringFrom("ubuntu:latest"),
			Command:        "echo hello",
			TimeoutSeconds: 3600,
			MaxRetries:     3,
			CronExpression: "0 12 * * *", // Different CronExpression
			IsActive:       true,
			Dependencies:   baseSchedule.Dependencies,
		}

		assert.False(t, baseSchedule.Equal(differentSchedule))
	})

	t.Run("different IsActive should not be equal", func(t *testing.T) {
		differentSchedule := &scheduler.Schedule{
			ScheduleID:     1,
			JobID:          100,
			JobName:        "Test Job",
			ImageName:      null.StringFrom("ubuntu:latest"),
			Command:        "echo hello",
			TimeoutSeconds: 3600,
			MaxRetries:     3,
			CronExpression: "0 0 * * *",
			IsActive:       false, // Different IsActive
			Dependencies:   baseSchedule.Dependencies,
		}

		assert.False(t, baseSchedule.Equal(differentSchedule))
	})

	t.Run("nil Schedule should not be equal", func(t *testing.T) {
		assert.False(t, baseSchedule.Equal(nil))
	})

	t.Run("different number of dependencies should not be equal", func(t *testing.T) {
		differentSchedule := &scheduler.Schedule{
			ScheduleID:     1,
			JobID:          100,
			JobName:        "Test Job",
			ImageName:      null.StringFrom("ubuntu:latest"),
			Command:        "echo hello",
			TimeoutSeconds: 3600,
			MaxRetries:     3,
			CronExpression: "0 0 * * *",
			IsActive:       true,
			Dependencies: []scheduler.JobDependency{
				{
					ID:                1,
					JobID:             100,
					DependsOn:         99,
					LookbackWindow:    3600,
					RequiredCondition: models.RcSuccess,
					MinWaitSeconds:    0,
				},
				{
					ID:                2,
					JobID:             100,
					DependsOn:         98,
					LookbackWindow:    3600,
					RequiredCondition: models.RcSuccess,
					MinWaitSeconds:    0,
				},
			},
		}

		assert.False(t, baseSchedule.Equal(differentSchedule))
	})

	t.Run("different dependency attributes should not be equal", func(t *testing.T) {
		differentSchedule := &scheduler.Schedule{
			ScheduleID:     1,
			JobID:          100,
			JobName:        "Test Job",
			ImageName:      null.StringFrom("ubuntu:latest"),
			Command:        "echo hello",
			TimeoutSeconds: 3600,
			MaxRetries:     3,
			CronExpression: "0 0 * * *",
			IsActive:       true,
			Dependencies: []scheduler.JobDependency{
				{
					ID:                1,
					JobID:             100,
					DependsOn:         99,
					LookbackWindow:    7200, // Different LookbackWindow
					RequiredCondition: models.RcSuccess,
					MinWaitSeconds:    0,
				},
			},
		}

		assert.False(t, baseSchedule.Equal(differentSchedule))
	})
}

func TestSchedule_ParentIDs(t *testing.T) {
	t.Run("returns all parent IDs", func(t *testing.T) {
		schedule := &scheduler.Schedule{
			ScheduleID: 1,
			JobID:      100,
			Dependencies: []scheduler.JobDependency{
				{
					ID:        1,
					JobID:     100,
					DependsOn: 98,
				},
				{
					ID:        2,
					JobID:     100,
					DependsOn: 99,
				},
			},
		}

		parentIDs := schedule.ParentIDs()
		assert.Len(t, parentIDs, 2)
		assert.Contains(t, parentIDs, int64(98))
		assert.Contains(t, parentIDs, int64(99))
	})

	t.Run("returns empty slice for no dependencies", func(t *testing.T) {
		schedule := &scheduler.Schedule{
			ScheduleID:   1,
			JobID:        100,
			Dependencies: []scheduler.JobDependency{},
		}

		parentIDs := schedule.ParentIDs()
		assert.Empty(t, parentIDs)
	})
}

func TestSchedule_HasDependencies(t *testing.T) {
	t.Run("returns true when dependencies exist", func(t *testing.T) {
		schedule := &scheduler.Schedule{
			ScheduleID: 1,
			JobID:      100,
			Dependencies: []scheduler.JobDependency{
				{
					ID:        1,
					JobID:     100,
					DependsOn: 99,
				},
			},
		}

		assert.True(t, schedule.HasDependencies())
	})

	t.Run("returns false when no dependencies", func(t *testing.T) {
		schedule := &scheduler.Schedule{
			ScheduleID:   1,
			JobID:        100,
			Dependencies: []scheduler.JobDependency{},
		}

		assert.False(t, schedule.HasDependencies())
	})
}
