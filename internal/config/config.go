package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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

	Queue struct {
		Host     string `mapstructure:"host"`
		Password string `mapstructure:"password"`
		DB       int    `mapstructure:"db"`
	} `mapstructure:"queue"`

	LogLevel zerolog.Level `mapstructure:"log_level"`

	Worker struct {
		SleepDuration int `mapstructure:"sleep_duration"`
	} `mapstructure:"worker"`
}

// LoadConfig reads the configuration from a file or environment variables
func LoadConfig(configPaths ...string) (*JRConfig, error) {
	// can specify config path from environment
	if path, exists := os.LookupEnv("JR_CONFIG_PATH"); exists {
		configPaths = append(configPaths, path)
	}
	for _, path := range configPaths {
		fi, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		mode := fi.Mode()
		switch {
		case mode.IsRegular():
			v := newViper()
			v.SetConfigFile(path)
			config, err := readConfig(v, path)
			if err != nil {
				continue
			}
			return config, nil

		case mode.IsDir():
			v := newViper()
			v.AddConfigPath(path)
			v.SetConfigName("config")
			v.SetConfigType("yaml")
			config, err := readConfig(v, path)
			if err != nil {
				continue
			}
			return config, nil
		}
	}

	v := newViper()
	// finally read from current working directory
	v.AddConfigPath(".")
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	cwd, _ := os.Getwd()

	config, err := readConfig(v, cwd)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// setDefaults sets default values for configuration
func newViper() *viper.Viper {
	v := viper.New()

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

	v.SetDefault("queue.host", "localhost:6379")
	v.SetDefault("queue.password", "redis")
	v.SetDefault("queue.db", 0)

	// Worker defaults
	v.SetDefault("worker.sleep_duration", 60)

	// Log level default
	v.SetDefault("log_level", "info")

	v.SetEnvPrefix("JR")                               // Prefix for environment variables
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // Replace dots with underscores in env vars
	v.AutomaticEnv()                                   // Read environment variables

	return v
}

func readConfig(v *viper.Viper, path string) (*JRConfig, error) {
	var config JRConfig

	if err := v.ReadInConfig(); err != nil {
		log.Warn().
			Str("path", path).
			Msg("Could not read config file")
		return nil, err
	}
	if err := v.Unmarshal(&config); err != nil {
		log.Warn().
			Str("path", path).
			Msg("Could not unmarshall config")
		return nil, err
	}

	return &config, nil
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
