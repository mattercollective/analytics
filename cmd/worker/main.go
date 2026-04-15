package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/mattercollective/analytics-engine/internal/api/response"
	"github.com/mattercollective/analytics-engine/internal/config"
	"github.com/mattercollective/analytics-engine/internal/database"
	"github.com/mattercollective/analytics-engine/internal/ingestion"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to load config")
	}

	if cfg.LogFormat == "text" {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	}

	pool, err := database.NewPool(ctx, cfg.DatabaseURL, cfg.DatabasePoolMin, cfg.DatabasePoolMax)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer pool.Close()

	metricsRepo := repository.NewMetricsRepo(pool)
	syncRepo := repository.NewSyncRepo(pool)
	engagementRepo := repository.NewEngagementRepo(pool)
	worker := ingestion.NewWorker(metricsRepo, syncRepo, engagementRepo, logger)

	orchestrator, err := ingestion.NewOrchestrator(ctx, cfg, worker, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize orchestrator")
	}

	// Worker HTTP server — receives Cloud Scheduler triggers
	r := chi.NewRouter()

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		response.JSON(w, http.StatusOK, map[string]string{"status": "ok", "role": "worker"})
	})

	// Cloud Scheduler hits these endpoints to trigger syncs
	r.Post("/internal/sync/{platform}", func(w http.ResponseWriter, r *http.Request) {
		platformID := chi.URLParam(r, "platform")
		logger.Info().Str("platform", platformID).Msg("sync triggered by scheduler")

		if err := orchestrator.SyncPlatform(r.Context(), platformID); err != nil {
			logger.Error().Err(err).Str("platform", platformID).Msg("sync failed")
			response.Error(w, http.StatusInternalServerError, err.Error())
			return
		}

		response.JSON(w, http.StatusOK, map[string]string{
			"status":   "completed",
			"platform": platformID,
		})
	})

	r.Post("/internal/maintenance/refresh-views", func(w http.ResponseWriter, r *http.Request) {
		logger.Info().Msg("refreshing materialized views")
		_, err := pool.Exec(r.Context(), "REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.asset_platform_daily")
		if err != nil {
			logger.Error().Err(err).Msg("view refresh failed")
			response.Error(w, http.StatusInternalServerError, err.Error())
			return
		}
		response.JSON(w, http.StatusOK, map[string]string{"status": "refreshed"})
	})

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Minute, // long timeout for sync jobs
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info().Str("port", cfg.Port).Msg("worker starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("worker server failed")
		}
	}()

	<-ctx.Done()
	logger.Info().Msg("worker shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("worker shutdown error")
	}

	fmt.Println("worker stopped")
}
