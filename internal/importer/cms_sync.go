package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// CMSSync pulls organizations, channels, and YouTube assets from the CMS
// Supabase project and syncs them into the analytics database.
type CMSSync struct {
	pool      *pgxpool.Pool
	apiURL    string
	apiKey    string
	logger    zerolog.Logger
}

func NewCMSSync(pool *pgxpool.Pool, apiURL, apiKey string, logger zerolog.Logger) *CMSSync {
	return &CMSSync{
		pool:   pool,
		apiURL: apiURL,
		apiKey: apiKey,
		logger: logger.With().Str("component", "cms_sync").Logger(),
	}
}

// SyncOrganizations pulls all orgs from CMS and upserts into public.clients.
func (c *CMSSync) SyncOrganizations(ctx context.Context) (int, error) {
	c.logger.Info().Msg("syncing organizations from CMS")

	type cmsOrg struct {
		ID        string  `json:"id"`
		Name      string  `json:"name"`
		Parent    *string `json:"parent"`
		CreatedAt string  `json:"created_at"`
	}

	orgs, err := fetchAll[cmsOrg](ctx, c.apiURL, c.apiKey, "organizations", "id,name,parent,created_at")
	if err != nil {
		return 0, fmt.Errorf("fetch organizations: %w", err)
	}

	c.logger.Info().Int("count", len(orgs)).Msg("fetched organizations from CMS")

	created := 0
	for _, org := range orgs {
		_, err := c.pool.Exec(ctx,
			`INSERT INTO public.clients (id, name, is_active, created_at)
			 VALUES ($1, $2, true, $3::timestamptz)
			 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()`,
			org.ID, org.Name, org.CreatedAt,
		)
		if err != nil {
			c.logger.Warn().Err(err).Str("org", org.Name).Msg("failed to upsert client")
			continue
		}
		created++
	}

	c.logger.Info().Int("synced", created).Msg("organizations synced to clients")
	return created, nil
}

// SyncChannels pulls YouTube channels from CMS and creates asset_identifiers (yt_channel_id).
func (c *CMSSync) SyncChannels(ctx context.Context) (int, error) {
	c.logger.Info().Msg("syncing YouTube channels from CMS")

	type cmsChannel struct {
		ID           string  `json:"id"`
		Title        string  `json:"title"`
		Organization *string `json:"organization"`
	}

	channels, err := fetchAll[cmsChannel](ctx, c.apiURL, c.apiKey, "channels", "id,title,organization")
	if err != nil {
		return 0, fmt.Errorf("fetch channels: %w", err)
	}

	c.logger.Info().Int("count", len(channels)).Msg("fetched channels from CMS")

	created := 0
	for _, ch := range channels {
		// Check if this channel ID already exists as an asset
		var exists bool
		c.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM public.asset_identifiers WHERE identifier_type = 'yt_channel_id' AND identifier_value = $1 AND effective_to IS NULL)`,
			ch.ID,
		).Scan(&exists)

		if exists {
			continue
		}

		// Create asset for the channel
		title := ch.Title
		if title == "" {
			title = "YouTube Channel " + ch.ID
		}

		tx, err := c.pool.Begin(ctx)
		if err != nil {
			continue
		}

		var assetID string
		err = tx.QueryRow(ctx,
			`INSERT INTO public.assets (asset_type, title) VALUES ('video', $1) RETURNING id`,
			title,
		).Scan(&assetID)
		if err != nil {
			tx.Rollback(ctx)
			continue
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO public.asset_identifiers (asset_id, identifier_type, identifier_value, source)
			 VALUES ($1, 'yt_channel_id', $2, 'cms_sync') ON CONFLICT DO NOTHING`,
			assetID, ch.ID,
		)
		if err != nil {
			tx.Rollback(ctx)
			continue
		}

		// Map to client if organization is set
		if ch.Organization != nil && *ch.Organization != "" {
			tx.Exec(ctx,
				`INSERT INTO analytics.client_assets (client_id, asset_id)
				 VALUES ($1, $2) ON CONFLICT DO NOTHING`,
				*ch.Organization, assetID,
			)
		}

		if err := tx.Commit(ctx); err != nil {
			continue
		}
		created++
	}

	c.logger.Info().Int("created", created).Msg("channels synced")
	return created, nil
}

// SyncYouTubeAssets pulls matter_assets from CMS and creates asset_identifiers (yt_asset_id).
// Also maps them to clients via analytics.client_assets.
func (c *CMSSync) SyncYouTubeAssets(ctx context.Context) (int, int, error) {
	c.logger.Info().Msg("syncing YouTube assets from CMS")

	type cmsAsset struct {
		AssetID      string  `json:"asset_id"`
		Organization *string `json:"organization"`
	}

	assets, err := fetchAll[cmsAsset](ctx, c.apiURL, c.apiKey, "matter_assets", "asset_id,organization")
	if err != nil {
		return 0, 0, fmt.Errorf("fetch matter_assets: %w", err)
	}

	c.logger.Info().Int("count", len(assets)).Msg("fetched YouTube assets from CMS")

	assetsCreated := 0
	mappingsCreated := 0
	batch := 0

	for _, asset := range assets {
		batch++

		// Check if already exists
		var existingAssetID *string
		c.pool.QueryRow(ctx,
			`SELECT asset_id::text FROM public.asset_identifiers WHERE identifier_type = 'yt_asset_id' AND identifier_value = $1 AND effective_to IS NULL LIMIT 1`,
			asset.AssetID,
		).Scan(&existingAssetID)

		if existingAssetID != nil {
			// Asset exists — just ensure client mapping
			if asset.Organization != nil && *asset.Organization != "" {
				_, err := c.pool.Exec(ctx,
					`INSERT INTO analytics.client_assets (client_id, asset_id)
					 VALUES ($1, $2::uuid) ON CONFLICT DO NOTHING`,
					*asset.Organization, *existingAssetID,
				)
				if err == nil {
					mappingsCreated++
				}
			}
			continue
		}

		// Create new asset
		tx, err := c.pool.Begin(ctx)
		if err != nil {
			continue
		}

		var newAssetID string
		err = tx.QueryRow(ctx,
			`INSERT INTO public.assets (asset_type, title) VALUES ('video', $1) RETURNING id`,
			"YT Asset "+asset.AssetID,
		).Scan(&newAssetID)
		if err != nil {
			tx.Rollback(ctx)
			continue
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO public.asset_identifiers (asset_id, identifier_type, identifier_value, source)
			 VALUES ($1, 'yt_asset_id', $2, 'cms_sync') ON CONFLICT DO NOTHING`,
			newAssetID, asset.AssetID,
		)
		if err != nil {
			tx.Rollback(ctx)
			continue
		}

		if asset.Organization != nil && *asset.Organization != "" {
			tx.Exec(ctx,
				`INSERT INTO analytics.client_assets (client_id, asset_id)
				 VALUES ($1, $2::uuid) ON CONFLICT DO NOTHING`,
				*asset.Organization, newAssetID,
			)
			mappingsCreated++
		}

		if err := tx.Commit(ctx); err != nil {
			continue
		}
		assetsCreated++

		if batch%5000 == 0 {
			c.logger.Info().Int("progress", batch).Int("total", len(assets)).Int("created", assetsCreated).Msg("YouTube asset sync progress")
		}
	}

	c.logger.Info().
		Int("assets_created", assetsCreated).
		Int("mappings_created", mappingsCreated).
		Msg("YouTube assets synced")

	return assetsCreated, mappingsCreated, nil
}

// fetchAll paginates through a Supabase REST API table and returns all rows.
func fetchAll[T any](ctx context.Context, apiURL, apiKey, table, selectCols string) ([]T, error) {
	var allRows []T
	offset := 0
	limit := 1000

	for {
		u := fmt.Sprintf("%s/rest/v1/%s?select=%s&limit=%d&offset=%d",
			apiURL, url.PathEscape(table), url.QueryEscape(selectCols), limit, offset)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("apikey", apiKey)
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Prefer", "count=exact")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
		}

		var rows []T
		if err := json.Unmarshal(body, &rows); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}

		allRows = append(allRows, rows...)

		// Check if we got fewer than limit (last page)
		if len(rows) < limit {
			break
		}

		// Also check Content-Range header for total
		cr := resp.Header.Get("Content-Range")
		if cr != "" {
			// Format: "0-999/12345"
			for i := len(cr) - 1; i >= 0; i-- {
				if cr[i] == '/' {
					total, _ := strconv.Atoi(cr[i+1:])
					if total > 0 && offset+limit >= total {
						return allRows, nil
					}
					break
				}
			}
		}

		offset += limit
	}

	return allRows, nil
}
