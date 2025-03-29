package scheduler

import (
	"github.com/guregu/null/v6"
	"jobrunner/internal/models"
)

// JobExecution refers to a single instance of the job run. When there are retries, it will still
// point to the same JobExecution.
type JobExecution struct {
	Id        int64                  `db:"id"`
	JobId     int64                  `db:"job_id"`
	Status    models.ExecutionStatus `db:"status"`
	StartTime null.Time              `db:"start_time"`
	EndTime   null.Time              `db:"end_time"`
	ExitCode  int                    `db:"exit_code"`
	Output    null.String            `db:"output"`
	Error     null.String            `db:"error"`
	Attempts  int                    `db:"attempts"`
	WorkerId  null.String            `db:"worker_id"`
}
