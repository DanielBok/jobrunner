package runcmd

import "github.com/spf13/cobra"

var Command = &cobra.Command{
	Use:   "run",
	Short: "Run service",
	Long:  "Run service from a selected list of services",
}

func init() {
	Command.AddCommand(workerCmd)
}
