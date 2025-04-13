package api

import (
	"context"
	"embed"
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"jobrunner/internal/scheduler"
)

type Server struct {
	ctx       context.Context
	db        *sqlx.DB
	scheduler *scheduler.TaskScheduler
	router    *chi.Mux
}

var frontendFS embed.FS

type Config struct {
	Host          string `mapstructure:"host"`
	Port          int    `mapstructure:"port"`
	ServeFrontend bool   `mapstructure:"serve_frontend"`
}

// New creates a new API server instance
func New(ctx context.Context, db *sqlx.DB, scheduler *scheduler.TaskScheduler, config *Config) *Server {
	s := &Server{
		ctx:       ctx,
		db:        db,
		scheduler: scheduler,
		router:    chi.NewRouter(),
	}

	// Set up middleware
	s.router.Use(middleware.Logger)
	s.router.Use(middleware.Recoverer)
	s.router.Use(middleware.RequestID)

	s.router.Route("/api", func(r chi.Router) {
		r.Mount("/task", NewTaskRouter(ctx, db, r))
	})

	if config.ServeFrontend {
		s.router.Handle("/*", http.FileServer(http.FS(frontendFS)))
	}

	return s
}

func readJson(w http.ResponseWriter, r *http.Request, payload any) error {
	defer func() {
		if err := r.Body.Close(); err != nil {
			log.Error().Err(err).Msg("Could not close request body")
		}
	}()

	err := json.NewDecoder(r.Body).Decode(payload)
	if err != nil {
		http.Error(w, "could not parse request body to payload", http.StatusBadRequest)
	}
	return err
}

func serveJson(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(payload)
	if err != nil {
		http.Error(w, "Failed to encode payload", http.StatusInternalServerError)
		log.Error().Err(err).Msg("JSON encoding issue")
	}
}

func releaseTx(tx *sqlx.Tx) {
	if err1 := tx.Commit(); err1 != nil {
		if err2 := tx.Rollback(); err2 != nil {
			log.Error().
				Err(err1).
				Err(err2).
				Msg("Error encountered when trying to release transaction")
		}
	}
}

func rollbackTx(tx *sqlx.Tx) {
	if err := tx.Rollback(); err != nil {
		log.Error().Err(err).Msg("Could not rollback transaction")
	}
}
