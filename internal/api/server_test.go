package api_test

import (
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

func mustClose(db *sqlx.DB) {
	err := db.Close()
	if err != nil {
		log.Fatal().Err(err).Msg("Could not close database")
	}
}
