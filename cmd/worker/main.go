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
	"github.com/mattercollective/analytics-engine/internal/importer"
	"github.com/mattercollective/analytics-engine/internal/ingestion"
	"github.com/mattercollective/analytics-engine/internal/platform/spotify"
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

	// Apple GCS reports import — triggered daily after apple-reporter uploads to GCS
	r.Post("/internal/import/apple-reports", func(w http.ResponseWriter, r *http.Request) {
		logger.Info().Msg("apple reports import triggered by scheduler")

		gcsClient, err := importer.NewGCSClient(r.Context())
		if err != nil {
			logger.Error().Err(err).Msg("failed to create GCS client")
			response.Error(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer gcsClient.Close()

		playlistRepo := repository.NewPlaylistRepo(pool)

		imp := importer.NewAppleReportsImporter(gcsClient, metricsRepo, playlistRepo, engagementRepo, logger, "matter-reports-raw")

		contentPath := "rebel-apple-reports/sales/amcontent/detailed/daily/AppleMusic_Content_93824149_20260316_V1_2.txt"
		if err := imp.LoadContentMapping(r.Context(), contentPath); err != nil {
			logger.Error().Err(err).Msg("failed to load Apple content mapping")
			response.Error(w, http.StatusInternalServerError, err.Error())
			return
		}

		var total int
		n1, _ := imp.ImportEditorialPlaylistAdds(r.Context(), "rebel-apple-reports/sales/ameditorialplaylistadds/")
		n2, _ := imp.ImportDemographics(r.Context(), "rebel-apple-reports/sales/amcontentdemographics/")
		n3, _ := imp.ImportShazams(r.Context(), "rebel-apple-reports/sales/amshazam/")
		n4, _ := imp.ImportContainers(r.Context(), "rebel-apple-reports/sales/amcontainer/")
		total = n1 + n2 + n3 + n4

		logger.Info().Int("playlist_adds", n1).Int("demographics", n2).Int("shazams", n3).Int("containers", n4).Msg("apple reports import complete")

		response.JSON(w, http.StatusOK, map[string]any{
			"status":        "completed",
			"playlist_adds": n1,
			"demographics":  n2,
			"shazams":       n3,
			"containers":    n4,
			"total":         total,
		})
	})

	// Apple streams import — triggered daily after apple-reporter uploads to GCS
	r.Post("/internal/import/apple-streams", func(w http.ResponseWriter, r *http.Request) {
		logger.Info().Msg("apple streams import triggered by scheduler")

		gcsClient, err := importer.NewGCSClient(r.Context())
		if err != nil {
			logger.Error().Err(err).Msg("failed to create GCS client")
			response.Error(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer gcsClient.Close()

		imp := importer.NewAppleImporter(gcsClient, metricsRepo, logger, "matter-reports-raw")

		contentPath := "rebel-apple-reports/sales/amcontent/detailed/daily/AppleMusic_Content_93824149_20260316_V1_2.txt"
		if err := imp.LoadContentMapping(r.Context(), contentPath); err != nil {
			logger.Error().Err(err).Msg("failed to load Apple content mapping")
			response.Error(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err := imp.ImportAllStreams(r.Context(), "rebel-apple-reports/sales/amstreams/daily/"); err != nil {
			logger.Error().Err(err).Msg("apple streams import failed")
			response.Error(w, http.StatusInternalServerError, err.Error())
			return
		}

		response.JSON(w, http.StatusOK, map[string]string{"status": "completed", "platform": "apple_streams"})
	})

	// Playlist polling — snapshots tracked playlists daily
	r.Post("/internal/playlists/poll", func(w http.ResponseWriter, r *http.Request) {
		logger.Info().Msg("playlist poll triggered by scheduler")

		playlistRepo := repository.NewPlaylistRepo(pool)

		// Get Spotify playlist fetcher from the orchestrator's registered fetcher
		var spotifyPF *spotify.PlaylistFetcher
		if fetcher := orchestrator.GetFetcher("spotify"); fetcher != nil {
			if sc, ok := fetcher.(*spotify.Client); ok {
				spotifyPF = sc.GetPlaylistFetcher()
			}
		}

		if spotifyPF == nil {
			logger.Warn().Msg("spotify adapter not available for playlist polling")
			response.Error(w, http.StatusServiceUnavailable, "spotify adapter not configured")
			return
		}

		poller := ingestion.NewPlaylistPoller(playlistRepo, metricsRepo, spotifyPF, logger)
		n, err := poller.PollAll(r.Context())
		if err != nil {
			logger.Error().Err(err).Msg("playlist poll failed")
			response.Error(w, http.StatusInternalServerError, err.Error())
			return
		}

		response.JSON(w, http.StatusOK, map[string]any{
			"status":    "completed",
			"positions": n,
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
