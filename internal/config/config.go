package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// JRConfig holds the application configuration
type JRConfig struct {
	Database struct {
		Host     string `mapstructure:"host"`
		Port     int    `mapstructure:"port"`
		User     string `mapstructure:"user"`
		Password string `mapstructure:"password"`
		Name     string `mapstructure:"name"`
		SSLMode  string `mapstructure:"sslmode"`
	} `mapstructure:"database"`

	Server struct {
		Host string `mapstructure:"host"`
		Port int    `mapstructure:"port"`
	} `mapstructure:"server"`

	Scheduler struct {
		MaxWorkers      int `mapstructure:"max_workers"`
		PollIntervalSec int `mapstructure:"poll_interval_sec"`
		JobTimeoutSec   int `mapstructure:"job_timeout_sec"`
	} `mapstructure:"scheduler"`

	LogLevel string `mapstructure:"log_level"`
}

// LoadConfig reads the configuration from a file or environment variables
func LoadConfig(configPath string) (*JRConfig, error) {
	v := viper.New()

	// Set default values
	setDefaults(v)

	// Setup viper to read environment variables
	v.SetEnvPrefix("JR")                               // Prefix for environment variables
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // Replace dots with underscores in env vars
	v.AutomaticEnv()                                   // Read environment variables

	// Configure viper to read from file
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		// Look for config in the config directory
		v.AddConfigPath(".")
		v.SetConfigName("config")
		v.SetConfigType("yaml")
	}

	// Read the configuration file
	if err := v.ReadInConfig(); err != nil {
		// It's okay if config file doesn't exist, we'll use defaults and env vars
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if !errors.As(err, &configFileNotFoundError) {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	// Unmarshal the configuration into the struct
	var config JRConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	return &config, nil
}

// setDefaults sets default values for configuration
func setDefaults(v *viper.Viper) {
	// Database defaults
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.user", "postgres")
	v.SetDefault("database.password", "postgres")
	v.SetDefault("database.name", "jobscheduler")
	v.SetDefault("database.sslmode", "disable")

	// Server defaults
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)

	// Scheduler defaults
	v.SetDefault("scheduler.max_workers", 10)
	v.SetDefault("scheduler.poll_interval_sec", 10)
	v.SetDefault("scheduler.job_timeout_sec", 3600) // 1 hour

	// Log level default
	v.SetDefault("log_level", "info")
}

// GetDatabaseURL returns a formatted database connection string
func (c *JRConfig) GetDatabaseURL() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.Database.User,
		c.Database.Password,
		c.Database.Host,
		c.Database.Port,
		c.Database.Name,
		c.Database.SSLMode,
	)
}
