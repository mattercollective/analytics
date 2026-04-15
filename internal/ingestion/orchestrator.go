package ingestion

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/mattercollective/analytics-engine/internal/config"
	"github.com/mattercollective/analytics-engine/internal/platform"
	"github.com/mattercollective/analytics-engine/internal/platform/amazon"
	"github.com/mattercollective/analytics-engine/internal/platform/apple"
	"github.com/mattercollective/analytics-engine/internal/platform/spotify"
	"github.com/mattercollective/analytics-engine/internal/platform/tiktok"
	"github.com/mattercollective/analytics-engine/internal/platform/youtube"
)

// Orchestrator manages platform adapters and dispatches sync jobs.
type Orchestrator struct {
	fetchers map[string]platform.Fetcher
	worker   *Worker
	logger   zerolog.Logger
}

// NewOrchestrator initializes all platform adapters from config.
func NewOrchestrator(ctx context.Context, cfg *config.Config, worker *Worker, logger zerolog.Logger) (*Orchestrator, error) {
	fetchers := make(map[string]platform.Fetcher)

	// Spotify
	if cfg.SpotifyClientID != "" && cfg.SpotifyClientSecret != "" {
		fetchers["spotify"] = spotify.NewClient(ctx, cfg.SpotifyClientID, cfg.SpotifyClientSecret, cfg.SpotifyLicensorID)
		logger.Info().Msg("spotify adapter initialized")
	}

	// Apple Music
	if cfg.AppleTeamID != "" && cfg.AppleKeyID != "" && cfg.ApplePrivateKey != "" {
		client, err := apple.NewClient(cfg.AppleTeamID, cfg.AppleKeyID, cfg.ApplePrivateKey)
		if err != nil {
			logger.Warn().Err(err).Msg("failed to initialize apple adapter")
		} else {
			fetchers["apple_music"] = client
			logger.Info().Msg("apple music adapter initialized")
		}
	}

	// YouTube
	if cfg.YouTubeServiceAccountJSON != "" && cfg.YouTubeContentOwnerID != "" {
		client, err := youtube.NewClient(ctx, cfg.YouTubeServiceAccountJSON, cfg.YouTubeContentOwnerID)
		if err != nil {
			logger.Warn().Err(err).Msg("failed to initialize youtube adapter")
		} else {
			fetchers["youtube"] = client
			logger.Info().Msg("youtube adapter initialized")
		}
	}

	// Amazon
	if cfg.AmazonAPIKey != "" {
		fetchers["amazon_music"] = amazon.NewClient(cfg.AmazonAPIKey, cfg.AmazonAPISecret)
		logger.Info().Msg("amazon adapter initialized")
	}

	// TikTok (CSV-only, always available)
	fetchers["tiktok"] = tiktok.NewClient()
	logger.Info().Msg("tiktok adapter initialized (CSV mode)")

	return &Orchestrator{
		fetchers: fetchers,
		worker:   worker,
		logger:   logger,
	}, nil
}

// SyncPlatform triggers a sync for a specific platform.
func (o *Orchestrator) SyncPlatform(ctx context.Context, platformID string) error {
	fetcher, ok := o.fetchers[platformID]
	if !ok {
		return fmt.Errorf("no adapter registered for platform: %s", platformID)
	}

	return o.worker.RunSync(ctx, fetcher)
}

// RegisteredPlatforms returns the list of initialized platform IDs.
func (o *Orchestrator) RegisteredPlatforms() []string {
	platforms := make([]string, 0, len(o.fetchers))
	for id := range o.fetchers {
		platforms = append(platforms, id)
	}
	return platforms
}
