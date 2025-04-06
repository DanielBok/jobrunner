package scheduler

import (
	"github.com/guregu/null/v6"
	"jobrunner/internal/models"
)

// TaskRun refers to a single instance of the task run. When there are retries, it will still
// point to the same TaskRun.
type TaskRun struct {
	Id            int64            `db:"id"`
	TaskID        int64            `db:"task_id"`
	Status        models.RunStatus `db:"status"`
	StartTime     null.Time        `db:"start_time"`
	EndTime       null.Time        `db:"end_time"`
	ExitCode      int              `db:"exit_code"`
	Output        null.String      `db:"output"`
	Error         null.String      `db:"error"`
	Attempts      int              `db:"attempts"`
	LastHeartbeat null.Time        `db:"last_heartbeat"`
	WorkerId      null.String      `db:"worker_id"`
}
