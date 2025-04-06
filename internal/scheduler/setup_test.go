package scheduler_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/guregu/null/v6"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"jobrunner/internal/config"
	"jobrunner/internal/models"
	"jobrunner/internal/queue"
)

// The test database
var db *sqlx.DB
var rq *MockQueueClient

type MockQueueClient struct {
	mock.Mock
}

func (m *MockQueueClient) Publish(ctx context.Context, message queue.TaskMessage) error {
	args := m.Called(ctx, message)
	return args.Error(0)
}

func (m *MockQueueClient) Subscribe(ctx context.Context, handler func(queue.TaskMessage) error) error {
	args := m.Called(ctx, handler)
	return args.Error(0)
}

func (m *MockQueueClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestMain(m *testing.M) {
	conf, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to read in config: %v", err)
	}

	db, err = sqlx.Connect("pgx", conf.GetDatabaseURL())
	if err != nil {
		log.Fatalf("Failed to connect to test database: %v", err)
	}

	rq = &MockQueueClient{}

	defer func() {
		err = db.Close()
		if err != nil {
			log.Fatalf("Error encountered when closing test database: %v", err)
		}
	}()

	_, err = db.Exec("TRUNCATE TABLE task.run, task.dependency, task.schedule, task.definition CASCADE")

	os.Exit(m.Run())
}

// Helper functions for test setup

// Clears the test database
func clearTestDB(t *testing.T) {
	// Clean test tables
	_, err := db.Exec("TRUNCATE TABLE task.run, task.dependency, task.schedule, task.definition CASCADE")
	require.NoError(t, err)
}

// Preloads the test database with some sample data
func preloadTestDB(t *testing.T) map[int64]int64 {
	idMap := make(map[int64]int64)
	for i := 1; i <= 3; i++ {
		id := insertTask(t, fmt.Sprintf("Task %d", i), fmt.Sprintf("echo Task %d", i))
		idMap[int64(i)] = id
	}

	for _, dep := range []models.TaskDependency{
		{TaskID: idMap[2], DependsOn: idMap[1], RequiredCondition: "success"},
		{TaskID: idMap[3], DependsOn: idMap[1], RequiredCondition: "success"},
		{TaskID: idMap[3], DependsOn: idMap[2], RequiredCondition: "success"},
	} {

		insertDependency(t, dep.TaskID, dep.DependsOn, dep.RequiredCondition, 3600, 0)
	}

	for _, exec := range []models.TaskSchedule{
		{TaskID: idMap[1], CronExpression: "0,15,30,45 * * * *"},
		{TaskID: idMap[2], CronExpression: "0,15,30,45 * * * *"},
		{TaskID: idMap[3], CronExpression: "0 * * * *"},
		{TaskID: idMap[3], CronExpression: "15 * * * *"},
		{TaskID: idMap[3], CronExpression: "30 * * * *"},
		{TaskID: idMap[3], CronExpression: "45 * * * *"},
	} {
		insertSchedule(t, exec.TaskID, exec.CronExpression)
	}

	return idMap
}

func insertTask(t *testing.T, name, command string) int64 {
	var id int64
	err := db.QueryRow(`
		INSERT INTO task.definition (name, command)
		VALUES ($1, $2)
		RETURNING id
	`, name, command).Scan(&id)
	require.NoError(t, err, "Could not insert task. name=%q command=%q", name, command)
	return id
}

func insertDependency(t *testing.T, taskID, dependsOn int64, condition models.RequiredCondition, lookbackWindow, minWaitTime int) int64 {
	var id int64
	err := db.QueryRow(`
		INSERT INTO task.dependency (task_id, depends_on, required_condition, lookback_window, min_wait_time)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, taskID, dependsOn, condition, lookbackWindow, minWaitTime).Scan(&id)
	require.NoError(t, err, "Could not insert dependency: task_id=%d depends_on=%d required_condition=%q", taskID, dependsOn, condition)
	return id
}

func insertSchedule(t *testing.T, taskID int64, cronExpression string) int64 {
	var id int64
	err := db.QueryRow(`
		INSERT INTO task.schedule (task_id, cron_expression)
		VALUES ($1, $2)
		RETURNING id
	`, taskID, cronExpression).Scan(&id)

	require.NoError(t, err, "Could not insert schedule: task_id=%d cron_expression=%q", taskID, cronExpression)
	return id
}

func insertExecution(t *testing.T, taskId int64, status models.RunStatus, exitCode int, startTime, endTime null.Time) int64 {
	var id int64

	err := db.QueryRow(`
		INSERT INTO task.run (task_id, status, exit_code, start_time, end_time)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, taskId, status, exitCode, startTime, endTime).Scan(&id)

	require.NoError(t, err, "Could not insert execution")
	return id
}

func insertTestExecutionWithNullEndTime(t *testing.T, db *sqlx.DB, taskID int64, status models.RunStatus, exitCode int) int64 {
	var id int64
	err := db.QueryRow(`
		INSERT INTO task.run (task_id, status, start_time, end_time, exit_code)
		VALUES ($1, $2, $3, NULL, $4)
		RETURNING id
	`, taskID, status, time.Now().Add(-time.Minute), exitCode).Scan(&id)
	require.NoError(t, err)
	return id
}
