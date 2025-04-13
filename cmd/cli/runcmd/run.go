package runcmd

import (
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
	"jobrunner/internal/config"
	"jobrunner/internal/database"
	"jobrunner/internal/queue"
)

var Command = &cobra.Command{
	Use:   "run",
	Short: "Run service",
	Long:  "Run service from a selected list of services",
}

func init() {
	Command.AddCommand(workerCmd)
}

func loadConfig(cmd *cobra.Command) *config.JRConfig {
	confLoc := cmd.InheritedFlags().Lookup("config")
	if confLoc.Changed {
		fileLoc, err := cmd.InheritedFlags().GetString("config")
		if err != nil {
			log.Fatalf("Could not get file location: %v", err)
		}

		conf, err := config.LoadConfig(fileLoc)
		if err != nil {
			log.Fatalf("Could not load config file: %v", err)
		}

		return conf
	} else {
		conf, err := config.LoadConfig()
		if err != nil {
			log.Fatalf("Could not load config file: %v", err)
		}
		return conf
	}
}

func mustDatabase(conf *config.JRConfig) *sqlx.DB {
	db, err := database.New(conf)
	if err != nil {
		log.Fatalf("Could not connect to database: %v", err)
	}

	return db
}

func mustQueue(conf *config.JRConfig) *queue.RedisClient {
	redis, err := queue.NewRedisClient(conf.Queue.Host, conf.Queue.Password, conf.Queue.DB)
	if err != nil {
		log.Fatalf("Could not connect to redis queue: %v", err)
	}
	return redis
}
