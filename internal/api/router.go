package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/mattercollective/analytics-engine/internal/api/handler"
	"github.com/mattercollective/analytics-engine/internal/api/middleware"
	"github.com/mattercollective/analytics-engine/internal/ingestion"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

// NewRouter builds the chi router with all routes and middleware.
func NewRouter(
	pool *pgxpool.Pool,
	orchestrator *ingestion.Orchestrator,
	apiKeys []string,
	corsOrigins []string,
	logger zerolog.Logger,
) *chi.Mux {
	r := chi.NewRouter()

	// Global middleware
	r.Use(chimiddleware.RealIP)
	r.Use(chimiddleware.RequestID)
	r.Use(middleware.RequestLogger(logger))
	r.Use(chimiddleware.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   corsOrigins,
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-API-Key"},
		AllowCredentials: false,
		MaxAge:           300,
	}))

	// Repos
	metricsRepo := repository.NewMetricsRepo(pool)
	syncRepo := repository.NewSyncRepo(pool)
	assetRepo := repository.NewAssetRepo(pool)
	revenueRepo := repository.NewRevenueRepo(pool)
	playlistRepo := repository.NewPlaylistRepo(pool)
	engagementRepo := repository.NewEngagementRepo(pool)

	// Handlers
	healthH := handler.NewHealthHandler(pool)
	analyticsH := handler.NewAnalyticsHandler(metricsRepo)
	assetsH := handler.NewAssetsHandler(assetRepo)
	syncH := handler.NewSyncHandler(syncRepo, orchestrator)
	platformsH := handler.NewPlatformsHandler(assetRepo)
	revenueH := handler.NewRevenueHandler(revenueRepo)
	artistsH := handler.NewArtistsHandler(metricsRepo)
	playlistsH := handler.NewPlaylistsHandler(playlistRepo)
	engagementH := handler.NewEngagementHandler(engagementRepo)

	// Public routes (no auth)
	r.Get("/health", healthH.Health)

	// Internal routes (for Cloud Scheduler — authenticated via OIDC, not API key)
	r.Route("/internal", func(r chi.Router) {
		r.Post("/sync/{platform}", syncH.Trigger)
		r.Post("/maintenance/refresh-views", func(w http.ResponseWriter, r *http.Request) {
			_, err := pool.Exec(r.Context(), "REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.asset_platform_daily")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		})
	})

	// API routes (API key required)
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(middleware.APIKeyAuth(apiKeys))

		// Analytics
		r.Get("/analytics/summary", analyticsH.Summary)
		r.Get("/analytics/top-assets", analyticsH.TopAssets)
		r.Get("/analytics/territories", analyticsH.Territories)
		r.Get("/analytics/by-artist", artistsH.ByArtist)
		r.Get("/analytics/top-artists", artistsH.TopArtists)

		// Assets
		r.Get("/assets", assetsH.List)
		r.Get("/assets/{id}", assetsH.Get)

		// Platforms
		r.Get("/platforms", platformsH.List)

		// Playlists
		r.Get("/playlists/for-asset", playlistsH.ForAsset)
		r.Get("/playlists/top", playlistsH.Top)
		r.Get("/playlists/tracked", playlistsH.Tracked)
		r.Post("/playlists/track", playlistsH.Track)
		r.Get("/playlists/{id}/history", playlistsH.History)

		// Engagement
		r.Get("/engagement/sources", engagementH.Sources)
		r.Get("/engagement/rates", engagementH.Rates)
		r.Get("/engagement/discovery", engagementH.Discovery)
		r.Get("/engagement/demographics", engagementH.Demographics)

		// Revenue
		r.Get("/revenue/summary", revenueH.Summary)
		r.Get("/revenue/by-source", revenueH.BySource)
		r.Get("/revenue/by-territory", revenueH.ByTerritory)
		r.Get("/revenue/by-platform", revenueH.ByPlatform)

		// Sync management
		r.Get("/sync/status", syncH.Status)
		r.Post("/sync/trigger/{platform}", syncH.Trigger)
		r.Get("/sync/runs", syncH.Runs)
	})

	return r
}
