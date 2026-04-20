package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// UnifyAssets merges the fragmented YouTube and ISRC asset pools.
//
// Current state: 368K YouTube assets + 1K ISRC assets, zero overlap.
// Same track has two separate UUIDs — one for YT views, one for streams.
//
// This migration:
//  1. Pulls CMS asset_reports_with_org view (ISRC + YT asset_id + organization)
//  2. For each (ISRC, YT asset_id) pair where both exist in our DB as separate
//     assets, merges the ISRC-only asset INTO the YouTube asset by:
//     - Migrating metrics, engagement, demographics, revenue rows to YT asset_id
//     - Moving the ISRC identifier to the YT asset
//     - Deleting the orphan ISRC asset
//  3. For (ISRC, YT asset_id) pairs where only the YT asset exists, adds the
//     ISRC as an identifier so future imports resolve to the unified asset.
//  4. Ensures client_assets mapping exists for every unified asset.
func (c *CMSSync) UnifyAssets(ctx context.Context) (UnifyResult, error) {
	var result UnifyResult
	c.logger.Info().Msg("starting asset unification")

	type isrcAssetOrg struct {
		AssetID      string `json:"asset_id"`
		DisplayISRC  string `json:"display_isrc"`
		YourISRC     string `json:"your_isrc"`
		OtherISRC    string `json:"other_isrc"`
		Organization string `json:"organization"`
	}

	// Paginate through CMS view
	var rows []isrcAssetOrg
	offset := 0
	limit := 1000

	for {
		u := fmt.Sprintf("%s/rest/v1/asset_reports_with_org?select=asset_id,display_isrc,your_isrc,other_isrc,organization&display_isrc=not.is.null&organization=not.is.null&limit=%d&offset=%d",
			c.apiURL, limit, offset)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return result, err
		}
		req.Header.Set("apikey", c.apiKey)
		req.Header.Set("Authorization", "Bearer "+c.apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return result, err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			return result, fmt.Errorf("CMS API error %d: %s", resp.StatusCode, string(body))
		}

		var page []isrcAssetOrg
		if err := json.Unmarshal(body, &page); err != nil {
			return result, err
		}

		rows = append(rows, page...)
		if len(page) < limit {
			break
		}
		offset += limit
	}

	c.logger.Info().Int("rows", len(rows)).Msg("fetched CMS asset/ISRC/org rows")

	// Build authoritative mapping: ISRC → (CMS asset_id, organization)
	type authoritative struct {
		CMSAssetID string
		OrgID      string
	}
	isrcToAuth := make(map[string]authoritative, len(rows))
	for _, row := range rows {
		if row.AssetID == "" || row.Organization == "" {
			continue
		}
		candidates := []string{row.DisplayISRC, row.YourISRC}
		for _, o := range strings.Fields(row.OtherISRC) {
			candidates = append(candidates, o)
		}
		for _, isrc := range candidates {
			isrc = strings.TrimSpace(isrc)
			if len(isrc) < 10 {
				continue
			}
			if _, exists := isrcToAuth[isrc]; !exists {
				isrcToAuth[isrc] = authoritative{CMSAssetID: row.AssetID, OrgID: row.Organization}
			}
		}
	}

	c.logger.Info().Int("unique_isrcs", len(isrcToAuth)).Msg("built authoritative ISRC map")

	processed := 0
	for isrc, auth := range isrcToAuth {
		processed++
		if processed%5000 == 0 {
			c.logger.Info().
				Int("processed", processed).
				Int("merged", result.AssetsMerged).
				Int("tagged", result.ISRCsAdded).
				Int("mapped", result.ClientMappingsAdded).
				Msg("unification progress")
		}

		// Find the UUID for the YouTube asset with this CMS asset_id
		var ytAssetUUID string
		err := c.pool.QueryRow(ctx,
			`SELECT asset_id FROM public.asset_identifiers
			 WHERE identifier_type = 'yt_asset_id' AND identifier_value = $1 AND effective_to IS NULL
			 LIMIT 1`,
			auth.CMSAssetID,
		).Scan(&ytAssetUUID)
		if err != nil {
			// No YT asset in our DB for this CMS entry — skip
			result.NoYTAsset++
			continue
		}

		// Find any existing ISRC-only asset for this ISRC
		var isrcAssetUUID string
		err = c.pool.QueryRow(ctx,
			`SELECT asset_id FROM public.asset_identifiers
			 WHERE identifier_type = 'isrc' AND identifier_value = $1 AND effective_to IS NULL
			 LIMIT 1`,
			isrc,
		).Scan(&isrcAssetUUID)

		if err == nil && isrcAssetUUID != ytAssetUUID {
			// Merge: migrate all data from isrcAssetUUID → ytAssetUUID, then delete isrcAssetUUID
			if err := c.mergeAssets(ctx, isrcAssetUUID, ytAssetUUID, isrc); err != nil {
				c.logger.Warn().Err(err).Str("isrc", isrc).Msg("merge failed")
				result.MergeFailures++
				continue
			}
			result.AssetsMerged++
		} else if err != nil {
			// No ISRC-only asset exists yet — just add ISRC as identifier on the YT asset
			_, err := c.pool.Exec(ctx,
				`INSERT INTO public.asset_identifiers (asset_id, identifier_type, identifier_value, source)
				 VALUES ($1, 'isrc', $2, 'cms_unify')
				 ON CONFLICT DO NOTHING`,
				ytAssetUUID, isrc,
			)
			if err == nil {
				result.ISRCsAdded++
			}
		}

		// Ensure client_assets mapping exists for the unified (YT) asset
		tag, err := c.pool.Exec(ctx,
			`INSERT INTO analytics.client_assets (client_id, asset_id)
			 VALUES ($1, $2)
			 ON CONFLICT DO NOTHING`,
			auth.OrgID, ytAssetUUID,
		)
		if err == nil && tag.RowsAffected() > 0 {
			result.ClientMappingsAdded++
		}
	}

	c.logger.Info().
		Int("assets_merged", result.AssetsMerged).
		Int("isrcs_added", result.ISRCsAdded).
		Int("client_mappings_added", result.ClientMappingsAdded).
		Int("no_yt_asset", result.NoYTAsset).
		Int("merge_failures", result.MergeFailures).
		Msg("asset unification complete")

	return result, nil
}

// mergeAssets migrates all analytics data from srcAsset into dstAsset, then deletes src.
// All operations run in a transaction so a failure rolls back cleanly.
func (c *CMSSync) mergeAssets(ctx context.Context, srcAsset, dstAsset, isrc string) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Migrate metrics — ON CONFLICT DO UPDATE handles duplicate keys by summing values
	_, err = tx.Exec(ctx,
		`INSERT INTO analytics.metrics (asset_id, platform_id, territory, metric_date, metric_type, value, value_decimal, external_id, created_at, updated_at)
		 SELECT $2, platform_id, territory, metric_date, metric_type, value, value_decimal, external_id, created_at, NOW()
		 FROM analytics.metrics WHERE asset_id = $1
		 ON CONFLICT (asset_id, platform_id, territory, metric_date, metric_type) DO UPDATE SET
		   value = analytics.metrics.value + EXCLUDED.value,
		   updated_at = NOW()`,
		srcAsset, dstAsset,
	)
	if err != nil {
		return fmt.Errorf("migrate metrics: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM analytics.metrics WHERE asset_id = $1`, srcAsset); err != nil {
		return fmt.Errorf("delete src metrics: %w", err)
	}

	// Migrate engagement
	_, err = tx.Exec(ctx,
		`INSERT INTO analytics.engagement (asset_id, platform_id, territory, engagement_date, source, source_uri, streams, saves, skips, completions, discovery, created_at, updated_at)
		 SELECT $2, platform_id, territory, engagement_date, source, source_uri, streams, saves, skips, completions, discovery, created_at, NOW()
		 FROM analytics.engagement WHERE asset_id = $1
		 ON CONFLICT (asset_id, platform_id, territory, engagement_date, source) DO UPDATE SET
		   streams = analytics.engagement.streams + EXCLUDED.streams,
		   saves = analytics.engagement.saves + EXCLUDED.saves,
		   skips = analytics.engagement.skips + EXCLUDED.skips,
		   completions = analytics.engagement.completions + EXCLUDED.completions,
		   discovery = analytics.engagement.discovery + EXCLUDED.discovery,
		   updated_at = NOW()`,
		srcAsset, dstAsset,
	)
	if err != nil {
		return fmt.Errorf("migrate engagement: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM analytics.engagement WHERE asset_id = $1`, srcAsset); err != nil {
		return fmt.Errorf("delete src engagement: %w", err)
	}

	// Migrate demographics
	_, err = tx.Exec(ctx,
		`INSERT INTO analytics.demographics (asset_id, platform_id, territory, demo_date, age_bucket, gender, streams, listeners, created_at, updated_at)
		 SELECT $2, platform_id, territory, demo_date, age_bucket, gender, streams, listeners, created_at, NOW()
		 FROM analytics.demographics WHERE asset_id = $1
		 ON CONFLICT (asset_id, platform_id, territory, demo_date, age_bucket, gender) DO UPDATE SET
		   streams = analytics.demographics.streams + EXCLUDED.streams,
		   listeners = analytics.demographics.listeners + EXCLUDED.listeners,
		   updated_at = NOW()`,
		srcAsset, dstAsset,
	)
	if err != nil {
		return fmt.Errorf("migrate demographics: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM analytics.demographics WHERE asset_id = $1`, srcAsset); err != nil {
		return fmt.Errorf("delete src demographics: %w", err)
	}

	// Migrate revenue
	_, err = tx.Exec(ctx,
		`INSERT INTO analytics.revenue (asset_id, platform_id, territory, revenue_date, revenue_type, revenue_source, currency, amount, amount_usd, content_type, external_id, created_at, updated_at)
		 SELECT $2, platform_id, territory, revenue_date, revenue_type, revenue_source, currency, amount, amount_usd, content_type, external_id, created_at, NOW()
		 FROM analytics.revenue WHERE asset_id = $1
		 ON CONFLICT (asset_id, platform_id, territory, revenue_date, revenue_type) DO UPDATE SET
		   amount = analytics.revenue.amount + EXCLUDED.amount,
		   amount_usd = COALESCE(analytics.revenue.amount_usd, 0) + COALESCE(EXCLUDED.amount_usd, 0),
		   updated_at = NOW()`,
		srcAsset, dstAsset,
	)
	if err != nil {
		return fmt.Errorf("migrate revenue: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM analytics.revenue WHERE asset_id = $1`, srcAsset); err != nil {
		return fmt.Errorf("delete src revenue: %w", err)
	}

	// Migrate playlist positions
	_, err = tx.Exec(ctx,
		`UPDATE analytics.playlist_positions SET asset_id = $2
		 WHERE asset_id = $1
		 AND NOT EXISTS (
		   SELECT 1 FROM analytics.playlist_positions p2
		   WHERE p2.playlist_id = analytics.playlist_positions.playlist_id
		     AND p2.asset_id = $2
		     AND p2.snapshot_date = analytics.playlist_positions.snapshot_date
		 )`,
		srcAsset, dstAsset,
	)
	if err != nil {
		return fmt.Errorf("migrate playlist positions: %w", err)
	}
	// Delete any remaining (duplicate) positions
	if _, err = tx.Exec(ctx, `DELETE FROM analytics.playlist_positions WHERE asset_id = $1`, srcAsset); err != nil {
		return fmt.Errorf("delete src playlist positions: %w", err)
	}

	// Migrate client_assets mappings (unique on (client_id, asset_id))
	_, err = tx.Exec(ctx,
		`INSERT INTO analytics.client_assets (client_id, asset_id)
		 SELECT client_id, $2 FROM analytics.client_assets WHERE asset_id = $1
		 ON CONFLICT DO NOTHING`,
		srcAsset, dstAsset,
	)
	if err != nil {
		return fmt.Errorf("migrate client_assets: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM analytics.client_assets WHERE asset_id = $1`, srcAsset); err != nil {
		return fmt.Errorf("delete src client_assets: %w", err)
	}

	// Move the ISRC identifier from src to dst
	if _, err = tx.Exec(ctx, `DELETE FROM public.asset_identifiers WHERE asset_id = $1`, srcAsset); err != nil {
		return fmt.Errorf("delete src identifiers: %w", err)
	}
	if _, err = tx.Exec(ctx,
		`INSERT INTO public.asset_identifiers (asset_id, identifier_type, identifier_value, source)
		 VALUES ($1, 'isrc', $2, 'cms_unify')
		 ON CONFLICT DO NOTHING`,
		dstAsset, isrc,
	); err != nil {
		return fmt.Errorf("add ISRC to dst: %w", err)
	}

	// Delete the orphaned source asset
	if _, err = tx.Exec(ctx, `DELETE FROM public.assets WHERE id = $1`, srcAsset); err != nil {
		return fmt.Errorf("delete src asset: %w", err)
	}

	return tx.Commit(ctx)
}

// UnifyResult reports what happened during the unification run.
type UnifyResult struct {
	AssetsMerged        int // ISRC asset merged into YT asset (analytics data migrated)
	ISRCsAdded          int // ISRC identifier added to YT asset (no prior ISRC asset existed)
	ClientMappingsAdded int // New client_assets rows created
	NoYTAsset           int // CMS had ISRC but no YT asset in our DB to merge into
	MergeFailures       int // Transaction failures
}
