package server

//
//import (
//	"database/sql"
//	"encoding/json"
//	"log"
//	"net/http"
//	"strconv"
//
//	"github.com/go-chi/chi/v5"
//	"github.com/go-chi/chi/v5/middleware"
//	"github.com/jmoiron/sqlx"
//	"jobrunner/internal/models"
//	"jobrunner/internal/scheduler"
//)
//
//// Server handles HTTP requests for job management
//type Server struct {
//	db        *sqlx.DB
//	scheduler *scheduler.Scheduler
//	router    *chi.Mux
//}
//
//// NewServer creates a new API server
//func NewServer(db *sqlx.DB, scheduler *scheduler.Scheduler) *Server {
//	s := &Server{
//		db:        db,
//		scheduler: scheduler,
//		router:    chi.NewRouter(),
//	}
//
//	// Set up middleware
//	s.router.Use(middleware.Logger)
//	s.router.Use(middleware.Recoverer)
//	s.router.Use(middleware.RequestID)
//
//	// Define routes
//	s.router.Route("/api", func(r chi.Router) {
//		// Jobs endpoints
//		r.Route("/jobs", func(r chi.Router) {
//			r.Get("/", s.listJobs)
//			r.Post("/", s.createJob)
//			r.Get("/{jobID}", s.getJob)
//			r.Put("/{jobID}", s.updateJob)
//			//r.Delete("/{jobID}", s.deleteJob)
//			//r.Post("/{jobID}/run", s.runJob)
//		})
//
//		// Schedules endpoints
//		r.Route("/schedules", func(r chi.Router) {
//			//r.Get("/job/{jobID}", s.getJobSchedules)
//			//r.Post("/job/{jobID}", s.createSchedule)
//			//r.Put("/{scheduleID}", s.updateSchedule)
//			//r.Delete("/{scheduleID}", s.deleteSchedule)
//		})
//
//		// Job executions endpoints
//		r.Route("/executions", func(r chi.Router) {
//			//r.Get("/job/{jobID}", s.getJobExecutions)
//			//r.Get("/{executionID}", s.getExecution)
//		})
//	})
//
//	return s
//}
//
//// ServeHTTP implements the http.Handler interface
//func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
//	s.router.ServeHTTP(w, r)
//}
//
//// Helper to parse job ID from URL
//func (s *Server) getJobIDParam(r *http.Request) (int64, error) {
//	jobIDStr := chi.URLParam(r, "jobID")
//	return strconv.ParseInt(jobIDStr, 10, 64)
//}
//
//// Helper to parse schedule ID from URL
//func (s *Server) getScheduleIDParam(r *http.Request) (int64, error) {
//	scheduleIDStr := chi.URLParam(r, "scheduleID")
//	return strconv.ParseInt(scheduleIDStr, 10, 64)
//}
//
//// Helper to parse execution ID from URL
//func (s *Server) getExecutionIDParam(r *http.Request) (int64, error) {
//	executionIDStr := chi.URLParam(r, "executionID")
//	return strconv.ParseInt(executionIDStr, 10, 64)
//}
//
//// Helper to respond with JSON
//func respondJSON(w http.ResponseWriter, status int, data interface{}) {
//	w.Header().Set("Content-Type", "application/json")
//	w.WriteHeader(status)
//	if data != nil {
//		if err := json.NewEncoder(w).Encode(data); err != nil {
//			http.Error(w, err.Error(), http.StatusInternalServerError)
//		}
//	}
//}
//
//// Helper to respond with error
//func respondError(w http.ResponseWriter, status int, message string) {
//	respondJSON(w, status, map[string]string{"error": message})
//}
//
//// List all jobs
//func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
//	var jobs []models.Job
//
//	err := s.db.Select(&jobs, `SELECT * FROM jobs ORDER BY id DESC`)
//	if err != nil {
//		respondError(w, http.StatusInternalServerError, "Failed to list jobs")
//		return
//	}
//
//	respondJSON(w, http.StatusOK, jobs)
//}
//
//// Get a specific job
//func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
//	jobID, err := s.getJobIDParam(r)
//	if err != nil {
//		respondError(w, http.StatusBadRequest, "Invalid job ID")
//		return
//	}
//
//	var job models.Job
//	err = s.db.Get(&job, `SELECT * FROM jobs WHERE id = $1`, jobID)
//	if err != nil {
//		if err == sql.ErrNoRows {
//			respondError(w, http.StatusNotFound, "Job not found")
//		} else {
//			respondError(w, http.StatusInternalServerError, "Failed to get job")
//		}
//		return
//	}
//
//	respondJSON(w, http.StatusOK, job)
//}
//
//// Create a new job
//func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
//	var job models.Job
//	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
//		respondError(w, http.StatusBadRequest, "Invalid request body")
//		return
//	}
//
//	// Validate required fields
//	if job.Name == "" || job.Command == "" {
//		respondError(w, http.StatusBadRequest, "Name and command are required")
//		return
//	}
//
//	// Set default values if not provided
//	if job.TimeoutSeconds <= 0 {
//		job.TimeoutSeconds = 3600 // 1 hour default
//	}
//
//	// Insert job into database
//	query := `
//		INSERT INTO jobs (name, description, command, timeout_seconds, max_retries, is_active)
//		VALUES ($1, $2, $3, $4, $5, $6)
//		RETURNING id, created_at, updated_at
//	`
//
//	err := s.db.QueryRowx(
//		query,
//		job.Name,
//		job.Description,
//		job.Command,
//		job.TimeoutSeconds,
//		job.MaxRetries,
//		true,
//	).Scan(&job.ID, &job.CreatedAt, &job.UpdatedAt)
//
//	if err != nil {
//		respondError(w, http.StatusInternalServerError, "Failed to create job")
//		return
//	}
//
//	// Register job with scheduler
//	if err := s.scheduler.RefreshJob(job.ID); err != nil {
//		// Log error but still return success
//		log.Printf("Failed to schedule new job %d: %v", job.ID, err)
//	}
//
//	respondJSON(w, http.StatusCreated, job)
//}
//
//// Update an existing job
//func (s *Server) updateJob(w http.ResponseWriter, r *http.Request) {
//	jobID, err := s.getJobIDParam(r)
//	if err != nil {
//		respondError(w, http.StatusBadRequest, "Invalid job ID")
//		return
//	}
//
//	var job models.Job
//	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
//		respondError(w, http.StatusBadRequest, "Invalid request body")
//		return
//	}
//
//	// Ensure job ID from URL matches payload
//	job.ID = jobID
//
//	// Update job in database
//	query := `
//		UPDATE jobs
//		SET name = $1, description = $2, command = $3,
//			timeout_seconds = $4, max_retries = $5, is_active = $6, updated_at = now()
//		WHERE id = $7
//		RETURNING updated_at
//	`
//
//	err = s.db.QueryRowx(
//		query,
//		job.Name,
//		job.Description,
//		job.Command,
//		job.TimeoutSeconds,
//		job.MaxRetries,
//		job.IsActive,
//		job.ID,
//	).Scan(&job.UpdatedAt)
//
//	if err != nil {
//		respondError(w, http.StatusInternalServerError, "Failed to update job")
//		return
//	}
//
//	// Update scheduler with changes
//	if err := s.scheduler.RefreshJob(job.ID); err != nil {
//		log.Printf("Failed to refresh job %d in scheduler: %v", job.ID)
//	}
//}
