package config_test

import (
	"os"
	"testing"

	"jobrunner/internal/config"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	// Test with a config string instead of a file
	configYaml := `
database:
  host: testhost
  port: 5433
  user: testuser
  password: testpass
  name: testdb
  sslmode: require

server:
  host: 127.0.0.1
  port: 9090

scheduler:
  max_workers: 5
  poll_interval_sec: 5
  job_timeout_sec: 1800

log_level: debug
`
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() {
		err := os.Remove(tmpFile.Name())
		assert.NoError(t, err)
	}()

	// Write the YAML content to the file
	if _, err := tmpFile.WriteString(configYaml); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	// Load the configuration from the temporary file
	cfg, err := config.LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Assert the configuration values match what we expect
	assert.Equal(t, "testhost", cfg.Database.Host)
	assert.Equal(t, 5433, cfg.Database.Port)
	assert.Equal(t, "testuser", cfg.Database.User)
	assert.Equal(t, "testpass", cfg.Database.Password)
	assert.Equal(t, "testdb", cfg.Database.Name)
	assert.Equal(t, "require", cfg.Database.SSLMode)

	assert.Equal(t, "127.0.0.1", cfg.Server.Host)
	assert.Equal(t, 9090, cfg.Server.Port)

	assert.Equal(t, 5, cfg.Scheduler.MaxWorkers)
	assert.Equal(t, 5, cfg.Scheduler.PollIntervalSec)
	assert.Equal(t, 1800, cfg.Scheduler.JobTimeoutSec)

	assert.Equal(t, "debug", cfg.LogLevel)

	// Test the database URL construction
	expectedURL := "postgres://testuser:testpass@testhost:5433/testdb?sslmode=require"
	assert.Equal(t, expectedURL, cfg.GetDatabaseURL())
}

func TestEnvironmentVariables(t *testing.T) {
	// Set environment variables
	assert.NoError(t, os.Setenv("JR_DATABASE_HOST", "envhost"))
	assert.NoError(t, os.Setenv("JR_DATABASE_PORT", "5434"))
	assert.NoError(t, os.Setenv("JR_SERVER_PORT", "9091"))
	assert.NoError(t, os.Setenv("JR_SCHEDULER_MAX_WORKERS", "15"))
	assert.NoError(t, os.Setenv("JR_LOG_LEVEL", "warn"))

	// Ensure we clear them afterwards
	defer func() {
		assert.NoError(t, os.Unsetenv("JR_DATABASE_HOST"))
		assert.NoError(t, os.Unsetenv("JR_DATABASE_PORT"))
		assert.NoError(t, os.Unsetenv("JR_SERVER_PORT"))
		assert.NoError(t, os.Unsetenv("JR_SCHEDULER_MAX_WORKERS"))
		assert.NoError(t, os.Unsetenv("JR_LOG_LEVEL"))
	}()

	// Create a temporary file with minimal config
	configYaml := `database: {}` // Empty database config to test env override

	tmpFile, err := os.CreateTemp("", "config-env-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() {
		err := os.Remove(tmpFile.Name())
		assert.NoError(t, err)
	}()

	if _, err := tmpFile.WriteString(configYaml); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	// Load the configuration
	cfg, err := config.LoadConfig(tmpFile.Name())
	assert.NoErrorf(t, err, "Failed to load configuration: %v", err)

	// Assert environment variables have precedence
	assert.Equal(t, "envhost", cfg.Database.Host)
	assert.Equal(t, 5434, cfg.Database.Port)
	assert.Equal(t, 9091, cfg.Server.Port)
	assert.Equal(t, 15, cfg.Scheduler.MaxWorkers)
	assert.Equal(t, "warn", cfg.LogLevel)
}
