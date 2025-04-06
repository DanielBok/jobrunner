package scheduler

import "github.com/guregu/null/v6"

// Schedule is a dataclass holding all necessary information to execute the task's particular schedule.
// A single task can have multiple schedules and this object represents a particular instance
type Schedule struct {
	ScheduleID     int64            `db:"schedule_id"`     // Schedule ID. One task can have multiple schedules
	TaskID         int64            `db:"task_id"`         // Task ID
	TaskName       string           `db:"name"`            // Task name
	ImageName      null.String      `db:"image_name"`      // Docker image name. If provided, task executes in a docker container
	Command        string           `db:"command"`         // The shell command to execute. If using docker, this is the [COMMAND] [ARG...] portion of the docker container run command
	TimeoutSeconds int              `db:"timeout_seconds"` // Maximum task duration per attempt. 1 retry is 1 new attempt
	MaxRetries     int              `db:"max_retries"`     // Number of times to retry at most
	CronExpression string           `db:"cron_expression"` // Task run cron expression. This is specified in IANA timezone format
	IsActive       bool             `db:"is_active"`       // Whether task is still active
	Dependencies   []TaskDependency `db:"-"`               // Task dependencies
	DependencyJSON []byte           `db:"dependencies"`    // JSON aggregation of the dependencies
}

// Equal checks that 2 Schedule are the same
func (s *Schedule) Equal(other *Schedule) bool {
	if other == nil {
		return false
	}

	return s.ScheduleID == other.ScheduleID &&
		s.TaskID == other.TaskID &&
		s.TaskName == other.TaskName &&
		s.ImageName == other.ImageName &&
		s.Command == other.Command &&
		s.TimeoutSeconds == other.TimeoutSeconds &&
		s.MaxRetries == other.MaxRetries &&
		s.CronExpression == other.CronExpression &&
		s.IsActive == other.IsActive &&
		compareDependencies(s.Dependencies, other.Dependencies)
}

// ParentIDs returns a list of parent task ID that this current Schedule is dependent on
func (s *Schedule) ParentIDs() (parentIDs []int64) {
	for _, dep := range s.Dependencies {
		parentIDs = append(parentIDs, dep.DependsOn)
	}
	return
}

// HasDependencies returns true if the task has dependencies, otherwise false
func (s *Schedule) HasDependencies() bool {
	return len(s.Dependencies) > 0
}

func compareDependencies(own []TaskDependency, other []TaskDependency) bool {
	ownMap := make(map[int64]TaskDependency)
	for _, dep := range own {
		ownMap[dep.ID] = dep
	}

	otherMap := make(map[int64]TaskDependency)
	for _, dep := range other {
		otherMap[dep.ID] = dep
	}

	if len(ownMap) != len(otherMap) {
		return false
	}

	for key, ownDep := range ownMap {
		otherDep, exists := otherMap[key]
		if !exists || !ownDep.Equal(&otherDep) {
			return false
		}
	}

	return true
}
