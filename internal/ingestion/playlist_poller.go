package ingestion

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"github.com/mattercollective/analytics-engine/internal/model"
	"github.com/mattercollective/analytics-engine/internal/platform/spotify"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

// PlaylistPoller polls tracked playlists and snapshots their contents daily.
type PlaylistPoller struct {
	playlistRepo *repository.PlaylistRepo
	metricsRepo  *repository.MetricsRepo
	spotifyPF    *spotify.PlaylistFetcher
	logger       zerolog.Logger
}

func NewPlaylistPoller(
	playlistRepo *repository.PlaylistRepo,
	metricsRepo *repository.MetricsRepo,
	spotifyPF *spotify.PlaylistFetcher,
	logger zerolog.Logger,
) *PlaylistPoller {
	return &PlaylistPoller{
		playlistRepo: playlistRepo,
		metricsRepo:  metricsRepo,
		spotifyPF:    spotifyPF,
		logger:       logger.With().Str("component", "playlist_poller").Logger(),
	}
}

// PollAll fetches all tracked playlists and snapshots their contents.
func (p *PlaylistPoller) PollAll(ctx context.Context) (int, error) {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	totalPositions := 0

	// Get all tracked playlists
	playlists, err := p.playlistRepo.ListTrackedPlaylists(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("list tracked playlists: %w", err)
	}

	p.logger.Info().Int("playlists", len(playlists)).Msg("starting playlist poll")

	for _, pl := range playlists {
		if pl.PlatformID != "spotify" {
			continue // Only Spotify poller for now
		}

		meta, tracks, err := p.spotifyPF.SnapshotPlaylist(ctx, pl.ExternalID, today)
		if err != nil {
			p.logger.Error().Err(err).Str("playlist", pl.Name).Msg("failed to snapshot playlist")
			continue
		}

		// Update playlist metadata (followers, track count, etc.)
		playlistID, err := p.playlistRepo.UpsertPlaylist(ctx, *meta)
		if err != nil {
			p.logger.Error().Err(err).Str("playlist", pl.Name).Msg("failed to upsert playlist")
			continue
		}

		// Update follower count history
		if meta.FollowerCount != nil {
			p.playlistRepo.UpsertFollowerSnapshot(ctx, playlistID, today, *meta.FollowerCount)
		}

		// Snapshot positions for tracks we can resolve
		var positions []model.PlaylistPosition
		for _, track := range tracks {
			if track.ISRC == "" {
				continue
			}

			assetID, err := p.metricsRepo.ResolveAssetID(ctx, "isrc", track.ISRC)
			if err != nil || assetID == nil {
				continue
			}

			pos := track.Position
			positions = append(positions, model.PlaylistPosition{
				PlaylistID:   playlistID,
				AssetID:      *assetID,
				SnapshotDate: today,
				Position:     &pos,
			})
		}

		if len(positions) > 0 {
			n, err := p.playlistRepo.BulkUpsertPositions(ctx, positions)
			if err != nil {
				p.logger.Error().Err(err).Str("playlist", pl.Name).Msg("failed to upsert positions")
				continue
			}
			totalPositions += n
		}

		p.logger.Info().
			Str("playlist", pl.Name).
			Int("tracks_total", len(tracks)).
			Int("tracks_resolved", len(positions)).
			Int64("followers", *meta.FollowerCount).
			Msg("playlist snapshot complete")
	}

	p.logger.Info().Int("total_positions", totalPositions).Msg("playlist poll complete")
	return totalPositions, nil
}
