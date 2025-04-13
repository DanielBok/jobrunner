package config

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// FromCobraCmd creates a JRConfig instance from a cobra command object. It will panic if any errors are
// raised.
func FromCobraCmd(cmd *cobra.Command) *JRConfig {
	var flags *pflag.FlagSet
	if cmd.Name() == "jrctl" {
		flags = cmd.PersistentFlags()
	} else {
		flags = cmd.InheritedFlags()
	}

	hasValue := flags.Lookup("config").Changed
	if hasValue {
		fileLoc, err := flags.GetString("config")
		if err != nil {
			log.Fatal().Err(err).Msg("Could not get file location")
		}

		conf, err := LoadConfig(fileLoc)
		if err != nil {
			log.Fatal().Err(err).Msg("Could not load config file")
		}
		return conf
	} else {
		conf, err := LoadConfig()
		if err != nil {
			log.Fatal().Err(err).Msg("Could not load config file")
		}
		return conf
	}
}
