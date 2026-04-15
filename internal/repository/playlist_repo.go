package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mattercollective/analytics-engine/internal/model"
)

type PlaylistRepo struct {
	pool *pgxpool.Pool
}

func NewPlaylistRepo(pool *pgxpool.Pool) *PlaylistRepo {
	return &PlaylistRepo{pool: pool}
}

// UpsertPlaylist creates or updates a playlist, returning its ID.
func (r *PlaylistRepo) UpsertPlaylist(ctx context.Context, p model.PlaylistUpsert) (uuid.UUID, error) {
	var id uuid.UUID
	err := r.pool.QueryRow(ctx,
		`INSERT INTO analytics.playlists (platform_id, external_id, name, description, curator_name, curator_type, follower_count, track_count, image_url, platform_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
		 ON CONFLICT (platform_id, external_id) DO UPDATE SET
		   name = EXCLUDED.name,
		   description = EXCLUDED.description,
		   curator_name = EXCLUDED.curator_name,
		   curator_type = EXCLUDED.curator_type,
		   follower_count = EXCLUDED.follower_count,
		   track_count = EXCLUDED.track_count,
		   image_url = EXCLUDED.image_url,
		   platform_url = EXCLUDED.platform_url,
		   updated_at = NOW()
		 RETURNING id`,
		p.PlatformID, p.ExternalID, p.Name, p.Description, p.CuratorName, p.CuratorType, p.FollowerCount, p.TrackCount, p.ImageURL, p.PlatformURL,
	).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("upsert playlist: %w", err)
	}
	return id, nil
}

// BulkUpsertPositions snapshots track positions for a playlist on a given date.
func (r *PlaylistRepo) BulkUpsertPositions(ctx context.Context, positions []model.PlaylistPosition) (int, error) {
	if len(positions) == 0 {
		return 0, nil
	}

	const batchSize = 500
	total := 0

	for i := 0; i < len(positions); i += batchSize {
		end := i + batchSize
		if end > len(positions) {
			end = len(positions)
		}
		batch := positions[i:end]

		var b strings.Builder
		args := make([]any, 0, len(batch)*5)

		b.WriteString(`INSERT INTO analytics.playlist_positions (playlist_id, asset_id, snapshot_date, position, added_date) VALUES `)

		for j, p := range batch {
			if j > 0 {
				b.WriteString(",")
			}
			offset := j * 5
			fmt.Fprintf(&b, "($%d::uuid, $%d::uuid, $%d::date, $%d::int, $%d::date)",
				offset+1, offset+2, offset+3, offset+4, offset+5)
			args = append(args, p.PlaylistID, p.AssetID, p.SnapshotDate, p.Position, p.AddedDate)
		}

		b.WriteString(` ON CONFLICT (playlist_id, asset_id, snapshot_date) DO UPDATE SET position = EXCLUDED.position`)

		tag, err := r.pool.Exec(ctx, b.String(), args...)
		if err != nil {
			return total, fmt.Errorf("bulk upsert positions: %w", err)
		}
		total += int(tag.RowsAffected())
	}

	return total, nil
}

// UpsertFollowerSnapshot records a playlist's follower count for a date.
func (r *PlaylistRepo) UpsertFollowerSnapshot(ctx context.Context, playlistID uuid.UUID, date time.Time, count int64) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO analytics.playlist_followers (playlist_id, snapshot_date, follower_count)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (playlist_id, snapshot_date) DO UPDATE SET follower_count = EXCLUDED.follower_count`,
		playlistID, date, count,
	)
	if err != nil {
		return fmt.Errorf("upsert follower snapshot: %w", err)
	}
	return nil
}

// GetPlaylistsForAsset returns all playlists an asset is on for a given date.
func (r *PlaylistRepo) GetPlaylistsForAsset(ctx context.Context, assetID uuid.UUID, platform *string, date time.Time) ([]model.PlaylistWithPosition, error) {
	var b strings.Builder
	args := []any{assetID, date}
	argIdx := 3

	b.WriteString(`SELECT p.id, p.platform_id, p.external_id, p.name, p.description, p.curator_name, p.curator_type,
		       p.follower_count, p.track_count, p.is_active, p.image_url, p.platform_url, p.created_at, p.updated_at,
		       pp.position, pp.added_date::text
		FROM analytics.playlist_positions pp
		JOIN analytics.playlists p ON p.id = pp.playlist_id
		WHERE pp.asset_id = $1 AND pp.snapshot_date = $2`)

	if platform != nil {
		fmt.Fprintf(&b, ` AND p.platform_id = $%d`, argIdx)
		args = append(args, *platform)
		argIdx++
	}

	b.WriteString(` ORDER BY p.follower_count DESC NULLS LAST`)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("get playlists for asset: %w", err)
	}
	defer rows.Close()

	var results []model.PlaylistWithPosition
	for rows.Next() {
		var pw model.PlaylistWithPosition
		if err := rows.Scan(
			&pw.ID, &pw.PlatformID, &pw.ExternalID, &pw.Name, &pw.Description,
			&pw.CuratorName, &pw.CuratorType, &pw.FollowerCount, &pw.TrackCount,
			&pw.IsActive, &pw.ImageURL, &pw.PlatformURL, &pw.CreatedAt, &pw.UpdatedAt,
			&pw.Position, &pw.AddedDate,
		); err != nil {
			return nil, fmt.Errorf("scan playlist with position: %w", err)
		}
		results = append(results, pw)
	}

	return results, rows.Err()
}

// GetPositionHistory returns position changes for an asset in a specific playlist.
func (r *PlaylistRepo) GetPositionHistory(ctx context.Context, playlistID, assetID uuid.UUID, startDate, endDate time.Time) ([]model.PositionPoint, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT pp.snapshot_date::text, pp.position
		 FROM analytics.playlist_positions pp
		 WHERE pp.playlist_id = $1 AND pp.asset_id = $2
		 AND pp.snapshot_date >= $3 AND pp.snapshot_date <= $4
		 ORDER BY pp.snapshot_date`,
		playlistID, assetID, startDate, endDate,
	)
	if err != nil {
		return nil, fmt.Errorf("get position history: %w", err)
	}
	defer rows.Close()

	var results []model.PositionPoint
	for rows.Next() {
		var pp model.PositionPoint
		pp.Present = true
		if err := rows.Scan(&pp.Date, &pp.Position); err != nil {
			return nil, fmt.Errorf("scan position point: %w", err)
		}
		results = append(results, pp)
	}

	return results, rows.Err()
}

// GetTopPlaylistsForClient returns playlists driving the most streams for a client's catalog.
func (r *PlaylistRepo) GetTopPlaylistsForClient(ctx context.Context, clientID uuid.UUID, startDate, endDate time.Time, limit int) ([]model.PlaylistAttribution, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT p.id, p.name, p.platform_id, p.curator_type, p.follower_count,
		        COUNT(DISTINCT pp.asset_id) AS asset_count,
		        COALESCE(SUM(m.value), 0) AS total_streams
		 FROM analytics.playlist_positions pp
		 JOIN analytics.playlists p ON p.id = pp.playlist_id
		 JOIN analytics.client_assets ca ON ca.asset_id = pp.asset_id AND ca.client_id = $1
		 LEFT JOIN analytics.metrics m ON m.asset_id = pp.asset_id
		   AND m.platform_id = p.platform_id
		   AND m.metric_date = pp.snapshot_date
		   AND m.metric_type = 'streams'
		 WHERE pp.snapshot_date >= $2 AND pp.snapshot_date <= $3
		 GROUP BY p.id, p.name, p.platform_id, p.curator_type, p.follower_count
		 ORDER BY total_streams DESC
		 LIMIT $4`,
		clientID, startDate, endDate, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("get top playlists for client: %w", err)
	}
	defer rows.Close()

	var results []model.PlaylistAttribution
	for rows.Next() {
		var pa model.PlaylistAttribution
		if err := rows.Scan(&pa.PlaylistID, &pa.PlaylistName, &pa.PlatformID,
			&pa.CuratorType, &pa.FollowerCount, &pa.AssetCount, &pa.TotalStreams); err != nil {
			return nil, fmt.Errorf("scan playlist attribution: %w", err)
		}
		results = append(results, pa)
	}

	return results, rows.Err()
}

// ListTrackedPlaylists returns all actively tracked playlists for a client.
func (r *PlaylistRepo) ListTrackedPlaylists(ctx context.Context, clientID *uuid.UUID) ([]model.Playlist, error) {
	var b strings.Builder
	args := make([]any, 0, 1)
	argIdx := 1

	b.WriteString(`SELECT p.id, p.platform_id, p.external_id, p.name, p.description, p.curator_name, p.curator_type,
		       p.follower_count, p.track_count, p.is_active, p.image_url, p.platform_url, p.created_at, p.updated_at
		FROM analytics.tracked_playlists tp
		JOIN analytics.playlists p ON p.id = tp.playlist_id
		WHERE tp.is_active = true`)

	if clientID != nil {
		fmt.Fprintf(&b, ` AND (tp.client_id = $%d OR tp.client_id IS NULL)`, argIdx)
		args = append(args, *clientID)
		argIdx++
	}

	b.WriteString(` ORDER BY p.name`)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list tracked playlists: %w", err)
	}
	defer rows.Close()

	var results []model.Playlist
	for rows.Next() {
		var p model.Playlist
		if err := rows.Scan(&p.ID, &p.PlatformID, &p.ExternalID, &p.Name, &p.Description,
			&p.CuratorName, &p.CuratorType, &p.FollowerCount, &p.TrackCount,
			&p.IsActive, &p.ImageURL, &p.PlatformURL, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan tracked playlist: %w", err)
		}
		results = append(results, p)
	}

	return results, rows.Err()
}

// AddTrackedPlaylist adds a playlist to the tracking list.
func (r *PlaylistRepo) AddTrackedPlaylist(ctx context.Context, playlistID uuid.UUID, clientID *uuid.UUID, addedBy string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO analytics.tracked_playlists (playlist_id, client_id, added_by)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (playlist_id, client_id) DO UPDATE SET is_active = true`,
		playlistID, clientID, addedBy,
	)
	if err != nil {
		return fmt.Errorf("add tracked playlist: %w", err)
	}
	return nil
}
