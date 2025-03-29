package scheduler_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/guregu/null/v6"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"jobrunner/internal/config"
	"jobrunner/internal/models"
)

// The test database
var db *sqlx.DB

func TestMain(m *testing.M) {
	conf, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to read in config: %v", err)
	}

	db, err = sqlx.Connect("pgx", conf.GetDatabaseURL())

	if err != nil {
		log.Fatalf("Failed to connect to test database: %v", err)
	}

	defer func() {
		err = db.Close()
		if err != nil {
			log.Fatalf("Error encountered when closing test database: %v", err)
		}
	}()

	_, err = db.Exec("TRUNCATE TABLE tasks.execution, tasks.dependency, tasks.schedule, tasks.job CASCADE")

	os.Exit(m.Run())
}

// Helper functions for test setup

// Clears the test database
func clearTestDB(t *testing.T) {
	// Clean test tables
	_, err := db.Exec("TRUNCATE TABLE tasks.execution, tasks.dependency, tasks.schedule, tasks.job CASCADE")
	require.NoError(t, err)
}

// Preloads the test database with some sample data
func preloadTestDB(t *testing.T) map[int64]int64 {
	idMap := make(map[int64]int64)
	for i := 1; i <= 3; i++ {
		id := insertJob(t, fmt.Sprintf("Job %d", i), fmt.Sprintf("echo Job %d", i))
		idMap[int64(i)] = id
	}

	for _, dep := range []models.TasksDependency{
		{JobID: idMap[2], DependsOn: idMap[1], RequiredCondition: "success"},
		{JobID: idMap[3], DependsOn: idMap[1], RequiredCondition: "success"},
		{JobID: idMap[3], DependsOn: idMap[2], RequiredCondition: "success"},
	} {

		insertDependency(t, dep.JobID, dep.DependsOn, dep.RequiredCondition, 3600, 0)
	}

	for _, exec := range []models.TasksSchedule{
		{JobID: idMap[1], CronExpression: "0,15,30,45 * * * *"},
		{JobID: idMap[2], CronExpression: "0,15,30,45 * * * *"},
		{JobID: idMap[3], CronExpression: "0 * * * *"},
		{JobID: idMap[3], CronExpression: "15 * * * *"},
		{JobID: idMap[3], CronExpression: "30 * * * *"},
		{JobID: idMap[3], CronExpression: "45 * * * *"},
	} {
		insertSchedule(t, exec.JobID, exec.CronExpression)
	}

	return idMap
}

func insertJob(t *testing.T, name, command string) int64 {
	var id int64
	err := db.QueryRow(`
		INSERT INTO tasks.job (name, command)
		VALUES ($1, $2)
		RETURNING id
	`, name, command).Scan(&id)
	require.NoError(t, err, "Could not insert job. name=%q command=%q", name, command)
	return id
}

func insertDependency(t *testing.T, jobID, dependsOn int64, requiredCondition string, lookbackWindow, minWaitTime int) int64 {
	var id int64
	err := db.QueryRow(`
		INSERT INTO tasks.dependency (job_id, depends_on, required_condition, lookback_window, min_wait_time)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, jobID, dependsOn, requiredCondition, lookbackWindow, minWaitTime).Scan(&id)
	require.NoError(t, err, "Could not insert dependency: job_id=%d depends_on=%d required_condition=%q", jobID, dependsOn, requiredCondition)
	return id
}

func insertSchedule(t *testing.T, jobID int64, cronExpression string) int64 {
	var id int64
	err := db.QueryRow(`
		INSERT INTO tasks.schedule (job_id, cron_expression)
		VALUES ($1, $2)
		RETURNING id
	`, jobID, cronExpression).Scan(&id)

	require.NoError(t, err, "Could not insert schedule: job_id=%d cron_expression=%q", jobID, cronExpression)
	return id
}

func insertExecution(t *testing.T, jobId int64, status string, exitCode int, startTime, endTime null.Time, depsMet bool) int64 {
	var id int64

	err := db.QueryRow(`
		INSERT INTO tasks.execution (job_id, status, exit_code, start_time, end_time, dependencies_met)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id
	`, jobId, status, exitCode, startTime, endTime, depsMet).Scan(&id)

	require.NoError(t, err, "Could not insert execution")
	return id
}

func insertTestExecutionWithNullEndTime(t *testing.T, db *sqlx.DB, jobID int64, status string, exitCode int) int64 {
	var id int64
	err := db.QueryRow(`
		INSERT INTO tasks.execution (job_id, status, start_time, end_time, exit_code, dependencies_met)
		VALUES ($1, $2, $3, NULL, $4, TRUE)
		RETURNING id
	`, jobID, status, time.Now().Add(-time.Minute), exitCode).Scan(&id)
	require.NoError(t, err)
	return id
}
