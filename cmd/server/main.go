package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/mattercollective/analytics-engine/internal/api"
	"github.com/mattercollective/analytics-engine/internal/config"
	"github.com/mattercollective/analytics-engine/internal/database"
	"github.com/mattercollective/analytics-engine/internal/ingestion"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Config
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to load config")
	}

	if cfg.LogLevel == "debug" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	if cfg.LogFormat == "text" {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	}

	// Database
	pool, err := database.NewPool(ctx, cfg.DatabaseURL, cfg.DatabasePoolMin, cfg.DatabasePoolMax)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer pool.Close()

	logger.Info().Msg("database connected")

	// Ingestion orchestrator (for sync trigger endpoints)
	metricsRepo := repository.NewMetricsRepo(pool)
	syncRepo := repository.NewSyncRepo(pool)
	engagementRepo := repository.NewEngagementRepo(pool)
	worker := ingestion.NewWorker(metricsRepo, syncRepo, engagementRepo, logger)

	orchestrator, err := ingestion.NewOrchestrator(ctx, cfg, worker, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize orchestrator")
	}

	// Router
	router := api.NewRouter(pool, orchestrator, cfg.APIKeys, cfg.CORSOrigins, logger)

	// Server
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start
	go func() {
		logger.Info().Str("port", cfg.Port).Msg("server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("server failed")
		}
	}()

	// Graceful shutdown
	<-ctx.Done()
	logger.Info().Msg("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("server shutdown error")
	}

	fmt.Println("server stopped")
}
