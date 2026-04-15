package ingestion

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/mattercollective/analytics-engine/internal/model"
	"github.com/mattercollective/analytics-engine/internal/platform"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

// Worker executes a single platform sync: fetch → resolve → UPSERT → audit.
type Worker struct {
	metricsRepo    *repository.MetricsRepo
	syncRepo       *repository.SyncRepo
	engagementRepo *repository.EngagementRepo
	logger         zerolog.Logger
}

func NewWorker(metricsRepo *repository.MetricsRepo, syncRepo *repository.SyncRepo, engagementRepo *repository.EngagementRepo, logger zerolog.Logger) *Worker {
	return &Worker{
		metricsRepo:    metricsRepo,
		syncRepo:       syncRepo,
		engagementRepo: engagementRepo,
		logger:         logger,
	}
}

// RunSync executes a full sync for a platform fetcher.
func (w *Worker) RunSync(ctx context.Context, fetcher platform.Fetcher) error {
	platformID := fetcher.Platform()
	scope := "all"

	log := w.logger.With().Str("platform", platformID).Logger()
	log.Info().Msg("starting sync")

	// Create audit record
	runID, err := w.syncRepo.CreateSyncRun(ctx, platformID, scope)
	if err != nil {
		return fmt.Errorf("create sync run: %w", err)
	}

	update := model.SyncRunUpdate{Status: "success"}
	defer func() {
		if err := w.syncRepo.CompleteSyncRun(ctx, runID, update); err != nil {
			log.Error().Err(err).Msg("failed to complete sync run")
		}
	}()

	// Load sync state
	state, err := w.syncRepo.GetSyncState(ctx, platformID, scope)
	if err != nil {
		update.Status = "error"
		update.ErrorMessage = strPtr(err.Error())
		return fmt.Errorf("get sync state: %w", err)
	}

	since := time.Now().UTC().AddDate(0, 0, -7) // default: last 7 days
	var cursor string

	if state != nil {
		if state.LastSyncAt != nil {
			since = *state.LastSyncAt
		}
		if state.CursorToken != nil {
			cursor = *state.CursorToken
		}
	}

	// Fetch loop
	var totalFetched, totalInserted, totalUpdated int
	var minDate, maxDate *time.Time

	for {
		select {
		case <-ctx.Done():
			update.Status = "partial"
			update.ErrorMessage = strPtr("context cancelled")
			return ctx.Err()
		default:
		}

		result, err := fetcher.FetchSince(ctx, since, cursor)
		if err != nil {
			update.Status = "error"
			update.ErrorMessage = strPtr(err.Error())
			update.ErrorCount++
			log.Error().Err(err).Msg("fetch failed")
			break
		}

		update.APICallsMade++
		totalFetched += len(result.Metrics)

		// Resolve identifiers to asset_ids and build upsert batch
		upserts, err := w.resolveMetrics(ctx, result.Metrics)
		if err != nil {
			log.Warn().Err(err).Msg("some metrics failed resolution")
		}

		if len(upserts) > 0 {
			ins, upd, err := w.metricsRepo.BulkUpsert(ctx, upserts)
			if err != nil {
				update.Status = "partial"
				update.ErrorMessage = strPtr(err.Error())
				update.ErrorCount++
				log.Error().Err(err).Msg("bulk upsert failed")
				break
			}
			totalInserted += ins
			totalUpdated += upd

			// Track date range
			for _, u := range upserts {
				if minDate == nil || u.MetricDate.Before(*minDate) {
					t := u.MetricDate
					minDate = &t
				}
				if maxDate == nil || u.MetricDate.After(*maxDate) {
					t := u.MetricDate
					maxDate = &t
				}
			}
		}

		log.Info().
			Int("fetched", len(result.Metrics)).
			Int("resolved", len(upserts)).
			Bool("has_more", result.HasMore).
			Msg("page processed")

		if !result.HasMore {
			break
		}

		cursor = result.NextCursor
	}

	// Engagement + Demographics sync (if platform supports it)
	if engFetcher, ok := fetcher.(platform.EngagementFetcher); ok && w.engagementRepo != nil {
		w.syncEngagement(ctx, engFetcher, platformID, since, log)
		w.syncDemographics(ctx, engFetcher, platformID, since, log)
	}

	// Update sync state
	now := time.Now().UTC()
	if err := w.syncRepo.UpsertSyncState(ctx, platformID, scope, now, maxDate, nil); err != nil {
		log.Error().Err(err).Msg("failed to update sync state")
	}

	// Finalize audit
	update.RowsFetched = totalFetched
	update.RowsInserted = totalInserted
	update.RowsUpdated = totalUpdated
	update.DataDateMin = minDate
	update.DataDateMax = maxDate

	log.Info().
		Int("fetched", totalFetched).
		Int("inserted", totalInserted).
		Int("updated", totalUpdated).
		Str("status", update.Status).
		Msg("sync complete")

	return nil
}

// resolveMetrics maps RawMetrics to MetricUpserts by resolving identifiers to asset_ids.
func (w *Worker) resolveMetrics(ctx context.Context, raw []platform.RawMetric) ([]model.MetricUpsert, error) {
	var upserts []model.MetricUpsert
	var resolveErrors int

	for _, rm := range raw {
		assetID, err := w.resolveAssetID(ctx, rm)
		if err != nil || assetID == nil {
			resolveErrors++
			continue
		}

		u := model.MetricUpsert{
			AssetID:    *assetID,
			PlatformID: w.currentPlatform(rm),
			MetricDate: rm.Date,
			MetricType: model.MetricType(rm.MetricType),
			Value:      rm.Value,
		}

		if rm.Territory != "" {
			u.Territory = &rm.Territory
		}
		if rm.ValueDecimal != nil {
			u.ValueDecimal = rm.ValueDecimal
		}
		if rm.ExternalID != "" {
			u.ExternalID = &rm.ExternalID
		}

		upserts = append(upserts, u)
	}

	if resolveErrors > 0 {
		return upserts, fmt.Errorf("%d metrics failed asset resolution", resolveErrors)
	}
	return upserts, nil
}

func (w *Worker) resolveAssetID(ctx context.Context, rm platform.RawMetric) (*uuid.UUID, error) {
	// Try ISRC first (most common)
	if rm.ISRC != "" {
		return w.metricsRepo.ResolveAssetID(ctx, "isrc", rm.ISRC)
	}
	// Try YouTube Asset ID
	if rm.YTAssetID != "" {
		return w.metricsRepo.ResolveAssetID(ctx, "yt_asset_id", rm.YTAssetID)
	}
	// Try UPC
	if rm.UPC != "" {
		return w.metricsRepo.ResolveAssetID(ctx, "upc", rm.UPC)
	}
	return nil, fmt.Errorf("no identifier available for resolution")
}

func (w *Worker) currentPlatform(rm platform.RawMetric) string {
	source, ok := rm.RawData["source"].(string)
	if ok {
		switch source {
		case "spotify_bulk":
			return "spotify"
		case "apple_analytics":
			return "apple_music"
		case "youtube_analytics":
			return "youtube"
		case "amazon_api":
			return "amazon_music"
		case "tiktok_csv":
			return "tiktok"
		}
	}
	return "unknown"
}

// syncEngagement fetches and stores source-level engagement data.
func (w *Worker) syncEngagement(ctx context.Context, fetcher platform.EngagementFetcher, platformID string, since time.Time, log zerolog.Logger) {
	log.Info().Msg("syncing engagement data")

	result, err := fetcher.FetchEngagement(ctx, since, "")
	if err != nil {
		log.Warn().Err(err).Msg("engagement fetch failed")
		return
	}

	if len(result.Records) == 0 {
		log.Info().Msg("no engagement records")
		return
	}

	// Resolve ISRCs to asset_ids
	var upserts []model.EngagementUpsert
	for _, rec := range result.Records {
		assetID, err := w.metricsRepo.ResolveAssetID(ctx, "isrc", rec.ISRC)
		if err != nil || assetID == nil {
			continue
		}

		u := model.EngagementUpsert{
			AssetID:        *assetID,
			PlatformID:     platformID,
			EngagementDate: rec.Date,
			Source:         rec.Source,
			Streams:        rec.Streams,
			Saves:          rec.Saves,
			Skips:          rec.Skips,
			Completions:    rec.Completions,
			Discovery:      rec.Discovery,
		}
		if rec.Territory != "" {
			u.Territory = &rec.Territory
		}
		if rec.SourceURI != "" {
			u.SourceURI = &rec.SourceURI
		}
		upserts = append(upserts, u)
	}

	if len(upserts) > 0 {
		n, err := w.engagementRepo.BulkUpsertEngagement(ctx, upserts)
		if err != nil {
			log.Error().Err(err).Msg("engagement upsert failed")
			return
		}
		log.Info().Int("records", n).Msg("engagement data synced")
	}
}

// syncDemographics fetches and stores age/gender breakdown data.
func (w *Worker) syncDemographics(ctx context.Context, fetcher platform.EngagementFetcher, platformID string, since time.Time, log zerolog.Logger) {
	log.Info().Msg("syncing demographics data")

	result, err := fetcher.FetchDemographics(ctx, since, "")
	if err != nil {
		log.Warn().Err(err).Msg("demographics fetch failed")
		return
	}

	if len(result.Records) == 0 {
		log.Info().Msg("no demographic records")
		return
	}

	var upserts []model.DemographicUpsert
	for _, rec := range result.Records {
		assetID, err := w.metricsRepo.ResolveAssetID(ctx, "isrc", rec.ISRC)
		if err != nil || assetID == nil {
			continue
		}

		u := model.DemographicUpsert{
			AssetID:    *assetID,
			PlatformID: platformID,
			DemoDate:   rec.Date,
			AgeBucket:  rec.AgeBucket,
			Gender:     rec.Gender,
			Streams:    rec.Streams,
			Listeners:  rec.Listeners,
		}
		if rec.Territory != "" {
			u.Territory = &rec.Territory
		}
		upserts = append(upserts, u)
	}

	if len(upserts) > 0 {
		n, err := w.engagementRepo.BulkUpsertDemographics(ctx, upserts)
		if err != nil {
			log.Error().Err(err).Msg("demographics upsert failed")
			return
		}
		log.Info().Int("records", n).Msg("demographics data synced")
	}
}

func strPtr(s string) *string { return &s }
