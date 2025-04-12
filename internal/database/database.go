package database

import (
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"jobrunner/internal/config"
)

func New(conf *config.JRConfig) (*sqlx.DB, error) {
	return sqlx.Connect("pgx", conf.GetDatabaseURL())
}
