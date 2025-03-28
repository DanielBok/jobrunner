package scheduler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// DependencyProbe is used to check if a JobSchedule has all its dependencies met
type DependencyProbe struct {
	db *sqlx.DB
}

type JobDependency struct {
	ID                int64  `db:"id" json:"id"`                                 // Job Dependency ID
	JobID             int64  `db:"job_id" json:"job_id"`                         // Job ID
	DependsOn         int64  `db:"depends_on" json:"depends_on"`                 // The parent which must be "completed" before this job can run
	LookbackWindow    int    `db:"lookback_window" json:"lookback_window"`       // How far back (in seconds) to lookback for the "complete" condition
	RequiredCondition string `db:"required_condition" json:"required_condition"` // The type of completion required to say dependency is met
	MinWaitSeconds    int    `db:"min_wait_seconds" json:"min_wait_seconds"`     // Extra time needed to wait after parent completion before job can run
}

// NewDependencyProbe creates a new dependency resolver
func NewDependencyProbe(db *sqlx.DB) *DependencyProbe {
	return &DependencyProbe{db: db}
}

// CheckDependencies verifies if all dependencies for a job execution are satisfied
func (dr *DependencyProbe) CheckDependencies(ctx context.Context, dependencies []JobDependency) (bool, error) {
	if len(dependencies) == 0 {
		return true, nil
	}

	var params []interface{}
	var clauses []string
	depMap := make(map[int64]JobDependency)
	for _, d := range dependencies {
		clauses = append(clauses, "(job_id = ? AND end_time > NOW() - MAKE_INTERVAL(secs => ?))")
		params = append(params, d.DependsOn, d.LookbackWindow)
		depMap[d.DependsOn] = d
	}

	query := dr.db.Rebind(fmt.Sprintf(`
	SELECT DISTINCT ON (job_id)
	    id, job_id, status, start_time, end_time, exit_code, output, error, attempts, dependencies_met, worker_id
	FROM tasks.execution
	WHERE %s
	ORDER BY job_id, end_time DESC NULLS LAST
	`, strings.Join(clauses, " OR ")))

	var parentExecutions []JobExecution
	err := dr.db.SelectContext(ctx, &parentExecutions, query, params...)
	if err != nil {
		return false, err
	}

	if len(parentExecutions) != len(dependencies) {
		return false, nil
	}

	now := time.Now()

	// Check that all job execution's exit code meets the
	for _, ex := range parentExecutions {
		parent := depMap[ex.JobId]

		// If the EndTime is not Valid or the EndTime is before the latest acceptable time
		if !ex.EndTime.Valid ||
			ex.EndTime.Time.Before(now.Add(-time.Second*time.Duration(parent.LookbackWindow))) {
			return false, nil
		}

		// success requires 0 exit code
		if (parent.RequiredCondition == "success" && ex.ExitCode != 0) ||
			// completion requires non '-1' exit code
			(parent.RequiredCondition == "completion" && ex.ExitCode == -1) ||
			// Failure requires positive non-zero exit code
			(parent.RequiredCondition == "failure" && ex.ExitCode <= 0) {
			return false, nil
		}
	}

	return true, nil
}
