package runcmd

import (
	"context"
	"log"

	"github.com/spf13/cobra"
	"jobrunner/internal/config"
	"jobrunner/internal/database"
	"jobrunner/internal/queue"
	"jobrunner/internal/worker"
)

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

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Runs a worker process",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Running worker process")
		conf := loadConfig(cmd)

		db, err := database.New(conf)
		if err != nil {
			log.Fatalf("Could not connect to database: %v", err)
		}

		redis, err := queue.NewRedisClient(conf.Queue.Host, conf.Queue.Password, conf.Queue.DB)
		if err != nil {
			log.Fatalf("Could not connect to redis queue: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		wrk := worker.New(ctx, db, redis)

		defer func() {
			if err := db.Close(); err != nil {
				log.Printf("Could not close db cleanly on shutdown: %v\n", err)
			}

			if err := redis.Close(); err != nil {
				log.Printf("Could not close redis queue cleanly on shutdown: %v\n", err)
			}

			cancel()
			wrk.Stop()
		}()

		if err := wrk.Start(); err != nil {
			log.Fatalf("Worker %s ran into problems: %v", wrk.ID, err)
		}
	},
}
