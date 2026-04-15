package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mattercollective/analytics-engine/internal/model"
)

type AssetRepo struct {
	pool *pgxpool.Pool
}

func NewAssetRepo(pool *pgxpool.Pool) *AssetRepo {
	return &AssetRepo{pool: pool}
}

// ListAssets returns assets with optional search and client filter.
func (r *AssetRepo) ListAssets(ctx context.Context, search *string, clientID *uuid.UUID, page, perPage int) ([]model.AssetWithIdentifiers, int, error) {
	var where []string
	args := make([]any, 0, 4)
	argIdx := 1

	if clientID != nil {
		where = append(where, fmt.Sprintf("a.id IN (SELECT asset_id FROM analytics.client_assets WHERE client_id = $%d)", argIdx))
		args = append(args, *clientID)
		argIdx++
	}

	if search != nil && *search != "" {
		where = append(where, fmt.Sprintf("to_tsvector('english', a.title) @@ plainto_tsquery('english', $%d)", argIdx))
		args = append(args, *search)
		argIdx++
	}

	whereClause := ""
	if len(where) > 0 {
		whereClause = "WHERE " + strings.Join(where, " AND ")
	}

	// Count
	var total int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM public.assets a %s", whereClause)
	if err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count assets: %w", err)
	}

	// Data
	offset := (page - 1) * perPage
	dataQuery := fmt.Sprintf(`
		SELECT a.id, a.asset_type, a.title, a.artist_name, a.is_active, a.created_at, a.updated_at,
		       ci.isrc, ci.upc, ci.yt_asset_id, ci.yt_channel_id
		FROM public.assets a
		LEFT JOIN public.asset_current_identifiers ci ON ci.asset_id = a.id
		%s
		ORDER BY a.updated_at DESC
		LIMIT $%d OFFSET $%d`, whereClause, argIdx, argIdx+1)
	args = append(args, perPage, offset)

	rows, err := r.pool.Query(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list assets: %w", err)
	}
	defer rows.Close()

	var results []model.AssetWithIdentifiers
	for rows.Next() {
		var a model.AssetWithIdentifiers
		if err := rows.Scan(&a.ID, &a.AssetType, &a.Title, &a.ArtistName, &a.IsActive,
			&a.CreatedAt, &a.UpdatedAt,
			&a.ISRC, &a.UPC, &a.YTAssetID, &a.YTChannelID); err != nil {
			return nil, 0, fmt.Errorf("scan asset: %w", err)
		}
		results = append(results, a)
	}

	return results, total, rows.Err()
}

// GetAsset returns a single asset by ID with identifiers.
func (r *AssetRepo) GetAsset(ctx context.Context, id uuid.UUID) (*model.AssetWithIdentifiers, error) {
	var a model.AssetWithIdentifiers
	err := r.pool.QueryRow(ctx,
		`SELECT a.id, a.asset_type, a.title, a.artist_name, a.is_active, a.created_at, a.updated_at,
		        ci.isrc, ci.upc, ci.yt_asset_id, ci.yt_channel_id
		 FROM public.assets a
		 LEFT JOIN public.asset_current_identifiers ci ON ci.asset_id = a.id
		 WHERE a.id = $1`,
		id,
	).Scan(&a.ID, &a.AssetType, &a.Title, &a.ArtistName, &a.IsActive,
		&a.CreatedAt, &a.UpdatedAt,
		&a.ISRC, &a.UPC, &a.YTAssetID, &a.YTChannelID)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get asset: %w", err)
	}
	return &a, nil
}

// ListPlatforms returns all active platforms with their sync status.
func (r *AssetRepo) ListPlatforms(ctx context.Context) ([]model.PlatformStatus, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT p.id, p.display_name, p.category, p.is_active,
		        ss.last_sync_at, ss.last_data_date::text,
		        (SELECT sr.status FROM analytics.sync_runs sr
		         WHERE sr.platform_id = p.id ORDER BY sr.started_at DESC LIMIT 1)
		 FROM public.platforms p
		 LEFT JOIN analytics.sync_state ss ON ss.platform_id = p.id AND ss.scope = 'all'
		 WHERE p.is_active = true
		 ORDER BY p.display_name`)
	if err != nil {
		return nil, fmt.Errorf("list platforms: %w", err)
	}
	defer rows.Close()

	var results []model.PlatformStatus
	for rows.Next() {
		var ps model.PlatformStatus
		if err := rows.Scan(&ps.ID, &ps.DisplayName, &ps.Category, &ps.IsActive,
			&ps.LastSyncAt, &ps.LastDataDate, &ps.SyncStatus); err != nil {
			return nil, fmt.Errorf("scan platform status: %w", err)
		}
		results = append(results, ps)
	}

	return results, rows.Err()
}
