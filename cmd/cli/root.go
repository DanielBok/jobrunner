package cli

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"jobrunner/cmd/cli/runcmd"
	"jobrunner/internal/config"
)

var RootCmd = &cobra.Command{
	Use:   "jrctl",
	Short: "JobRunner - A simple job scheduling system",
	Long: `JobRunner is a simple scheduling system that executes task based on cron schedules. 

At a minimum, you need to start the scheduler, at least 1 worker and the webserver.`,
}

func init() {
	RootCmd.PersistentFlags().StringP("config", "c", "", "config file path")
	RootCmd.AddCommand(runcmd.Command)

	conf := config.FromCobraCmd(RootCmd)
	zerolog.SetGlobalLevel(conf.LogLevel)
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v", err)
		os.Exit(1)
	}
}
