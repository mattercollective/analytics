package importer

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// CMSSyncBatch is an optimized version that uses batch SQL for YouTube assets.
type CMSSyncBatch struct {
	pool   *pgxpool.Pool
	apiURL string
	apiKey string
	logger zerolog.Logger
}

func NewCMSSyncBatch(pool *pgxpool.Pool, apiURL, apiKey string, logger zerolog.Logger) *CMSSyncBatch {
	return &CMSSyncBatch{
		pool:   pool,
		apiURL: apiURL,
		apiKey: apiKey,
		logger: logger.With().Str("component", "cms_sync_batch").Logger(),
	}
}

// cmsYTAsset represents a YouTube asset from the CMS.
type cmsYTAsset struct {
	AssetID      string  `json:"asset_id"`
	Organization *string `json:"organization"`
}

// SyncYouTubeAssetsBatch pulls all matter_assets and inserts them in batches.
func (c *CMSSyncBatch) SyncYouTubeAssetsBatch(ctx context.Context) (int, int, error) {
	c.logger.Info().Msg("fetching YouTube assets from CMS (batch mode)")

	assets, err := fetchAll[cmsYTAsset](ctx, c.apiURL, c.apiKey, "matter_assets", "asset_id,organization")
	if err != nil {
		return 0, 0, fmt.Errorf("fetch matter_assets: %w", err)
	}

	c.logger.Info().Int("total", len(assets)).Msg("fetched YouTube assets, starting batch insert")

	batchSize := 500
	totalCreated := 0
	totalMappings := 0

	for i := 0; i < len(assets); i += batchSize {
		end := i + batchSize
		if end > len(assets) {
			end = len(assets)
		}
		batch := assets[i:end]

		created, mappings, err := c.insertBatch(ctx, batch)
		if err != nil {
			c.logger.Warn().Err(err).Int("batch_start", i).Msg("batch insert error, continuing")
			continue
		}
		totalCreated += created
		totalMappings += mappings

		if (i/batchSize+1)%20 == 0 {
			c.logger.Info().
				Int("progress", end).
				Int("total", len(assets)).
				Int("created", totalCreated).
				Int("mappings", totalMappings).
				Msg("batch progress")
		}
	}

	c.logger.Info().
		Int("assets_created", totalCreated).
		Int("mappings_created", totalMappings).
		Msg("YouTube assets batch sync complete")

	return totalCreated, totalMappings, nil
}

func (c *CMSSyncBatch) insertBatch(ctx context.Context, assets []cmsYTAsset) (int, int, error) {
	if len(assets) == 0 {
		return 0, 0, nil
	}

	// Step 1: Filter out asset_ids that already exist
	ytIDs := make([]string, len(assets))
	for i, a := range assets {
		ytIDs[i] = a.AssetID
	}

	rows, err := c.pool.Query(ctx,
		`SELECT identifier_value FROM public.asset_identifiers
		 WHERE identifier_type = 'yt_asset_id' AND identifier_value = ANY($1) AND effective_to IS NULL`,
		ytIDs,
	)
	if err != nil {
		return 0, 0, err
	}

	existing := make(map[string]bool)
	for rows.Next() {
		var v string
		rows.Scan(&v)
		existing[v] = true
	}
	rows.Close()

	// Filter to only new assets
	type newAsset struct {
		ytID string
		org  *string
	}
	var toInsert []newAsset
	for _, a := range assets {
		if !existing[a.AssetID] {
			toInsert = append(toInsert, newAsset{ytID: a.AssetID, org: a.Organization})
		}
	}

	if len(toInsert) == 0 {
		// Still create client_asset mappings for existing assets
		mappings := c.createMappingsForExisting(ctx, assets)
		return 0, mappings, nil
	}

	// Step 2: Batch insert assets
	var assetSQL strings.Builder
	assetArgs := make([]any, 0, len(toInsert))
	assetSQL.WriteString(`INSERT INTO public.assets (asset_type, title) VALUES `)

	for i, a := range toInsert {
		if i > 0 {
			assetSQL.WriteString(",")
		}
		fmt.Fprintf(&assetSQL, "('video', $%d)", i+1)
		assetArgs = append(assetArgs, "YT Asset "+a.ytID)
	}
	assetSQL.WriteString(" RETURNING id")

	assetRows, err := c.pool.Query(ctx, assetSQL.String(), assetArgs...)
	if err != nil {
		return 0, 0, fmt.Errorf("batch insert assets: %w", err)
	}

	var newAssetIDs []string
	for assetRows.Next() {
		var id string
		assetRows.Scan(&id)
		newAssetIDs = append(newAssetIDs, id)
	}
	assetRows.Close()

	if len(newAssetIDs) != len(toInsert) {
		return 0, 0, fmt.Errorf("expected %d asset IDs, got %d", len(toInsert), len(newAssetIDs))
	}

	// Step 3: Batch insert identifiers
	var idSQL strings.Builder
	idArgs := make([]any, 0, len(toInsert)*2)
	idSQL.WriteString(`INSERT INTO public.asset_identifiers (asset_id, identifier_type, identifier_value, source) VALUES `)

	for i, a := range toInsert {
		if i > 0 {
			idSQL.WriteString(",")
		}
		fmt.Fprintf(&idSQL, "($%d::uuid, 'yt_asset_id', $%d, 'cms_sync')", i*2+1, i*2+2)
		idArgs = append(idArgs, newAssetIDs[i], a.ytID)
	}
	idSQL.WriteString(" ON CONFLICT DO NOTHING")

	_, err = c.pool.Exec(ctx, idSQL.String(), idArgs...)
	if err != nil {
		return 0, 0, fmt.Errorf("batch insert identifiers: %w", err)
	}

	// Step 4: Batch insert client_asset mappings
	mappings := 0
	var mapSQL strings.Builder
	mapArgs := make([]any, 0)
	mapIdx := 1
	first := true

	for i, a := range toInsert {
		if a.org != nil && *a.org != "" {
			if first {
				mapSQL.WriteString(`INSERT INTO analytics.client_assets (client_id, asset_id) VALUES `)
				first = false
			} else {
				mapSQL.WriteString(",")
			}
			fmt.Fprintf(&mapSQL, "($%d::uuid, $%d::uuid)", mapIdx, mapIdx+1)
			mapArgs = append(mapArgs, *a.org, newAssetIDs[i])
			mapIdx += 2
			mappings++
		}
	}

	if !first {
		mapSQL.WriteString(" ON CONFLICT DO NOTHING")
		_, err = c.pool.Exec(ctx, mapSQL.String(), mapArgs...)
		if err != nil {
			c.logger.Warn().Err(err).Msg("batch mapping insert error")
		}
	}

	return len(toInsert), mappings, nil
}

func (c *CMSSyncBatch) createMappingsForExisting(ctx context.Context, assets []cmsYTAsset) int {
	mappings := 0
	for _, a := range assets {
		if a.Organization == nil || *a.Organization == "" {
			continue
		}

		var assetID string
		err := c.pool.QueryRow(ctx,
			`SELECT asset_id::text FROM public.asset_identifiers WHERE identifier_type = 'yt_asset_id' AND identifier_value = $1 AND effective_to IS NULL LIMIT 1`,
			a.AssetID,
		).Scan(&assetID)
		if err != nil {
			continue
		}

		_, err = c.pool.Exec(ctx,
			`INSERT INTO analytics.client_assets (client_id, asset_id) VALUES ($1::uuid, $2::uuid) ON CONFLICT DO NOTHING`,
			*a.Organization, assetID,
		)
		if err == nil {
			mappings++
		}
	}
	return mappings
}
