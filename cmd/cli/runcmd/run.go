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
	Command.AddCommand(schedulerCmd)
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
