package runcmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"jobrunner/internal/config"
	"jobrunner/internal/scheduler"
)

var schedulerCmd = &cobra.Command{
	Use:   "scheduler",
	Short: "Starts the scheduler process",
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().Msg("Running scheduler process")
		conf := config.FromCobraCmd(cmd)

		db := mustDatabase(conf)
		queue := mustQueue(conf)

		ctx, cancel := context.WithCancel(context.Background())
		sch := scheduler.New(db, queue)

		defer func() {
			if err := db.Close(); err != nil {
				log.Printf("Could not close db cleanly on shutdown: %v\n", err)
			}

			if err := queue.Close(); err != nil {
				log.Printf("Could not close redis queue cleanly on shutdown: %v\n", err)
			}

			cancel()
			sch.Stop()
		}()

		if err := sch.Start(ctx); err != nil {
			log.Fatal().Err(err).Msg("Failed to start scheduler")
		}

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

		log.Info().Msgf("Received signal %v, shutting down...", <-sigCh)
	},
}
