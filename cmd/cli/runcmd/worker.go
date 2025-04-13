package runcmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"jobrunner/internal/config"
	"jobrunner/internal/worker"
)

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Runs a worker process",
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().Msg("Running worker process")
		conf := config.FromCobraCmd(cmd)

		db := mustDatabase(conf)
		queue := mustQueue(conf)

		ctx, cancel := context.WithCancel(context.Background())
		wrk := worker.New(db, queue)
		wrk.NoMessageSleepDuration = conf.Worker.SleepDuration

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

		errCh := make(chan error, 1)
		go func() {
			errCh <- wrk.Start(ctx)
		}()

		defer func() {
			if err := db.Close(); err != nil {
				log.Printf("Could not close db cleanly on shutdown: %v\n", err)
			}

			if err := queue.Close(); err != nil {
				log.Printf("Could not close redis queue cleanly on shutdown: %v\n", err)
			}

			cancel()
		}()

		select {
		case err := <-errCh:
			if err != nil {
				log.Fatal().Err(err).Str("worker_id", wrk.ID).Msg("Ran into problems")
			}
		case sig := <-sigCh:
			log.Info().Msgf("Received signal %v, shutting down...", sig)
		}
	},
}
