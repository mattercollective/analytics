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

type EngagementRepo struct {
	pool *pgxpool.Pool
}

func NewEngagementRepo(pool *pgxpool.Pool) *EngagementRepo {
	return &EngagementRepo{pool: pool}
}

// BulkUpsertEngagement inserts or updates engagement records.
func (r *EngagementRepo) BulkUpsertEngagement(ctx context.Context, records []model.EngagementUpsert) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	const batchSize = 500
	total := 0

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		batch := records[i:end]

		var b strings.Builder
		args := make([]any, 0, len(batch)*11)

		b.WriteString(`INSERT INTO analytics.engagement (asset_id, platform_id, territory, engagement_date, source, source_uri, streams, saves, skips, completions, discovery, updated_at) VALUES `)

		for j, e := range batch {
			if j > 0 {
				b.WriteString(",")
			}
			offset := j * 11
			fmt.Fprintf(&b, "($%d::uuid, $%d::text, $%d::char(2), $%d::date, $%d::text, $%d::text, $%d, $%d, $%d, $%d, $%d, NOW())",
				offset+1, offset+2, offset+3, offset+4, offset+5, offset+6, offset+7, offset+8, offset+9, offset+10, offset+11)
			args = append(args, e.AssetID, e.PlatformID, e.Territory, e.EngagementDate, e.Source, e.SourceURI, e.Streams, e.Saves, e.Skips, e.Completions, e.Discovery)
		}

		b.WriteString(` ON CONFLICT (asset_id, platform_id, territory, engagement_date, source) DO UPDATE SET
			streams = EXCLUDED.streams, saves = EXCLUDED.saves, skips = EXCLUDED.skips,
			completions = EXCLUDED.completions, discovery = EXCLUDED.discovery, updated_at = NOW()`)

		tag, err := r.pool.Exec(ctx, b.String(), args...)
		if err != nil {
			return total, fmt.Errorf("bulk upsert engagement: %w", err)
		}
		total += int(tag.RowsAffected())
	}

	return total, nil
}

// BulkUpsertDemographics inserts or updates demographic records.
func (r *EngagementRepo) BulkUpsertDemographics(ctx context.Context, records []model.DemographicUpsert) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	const batchSize = 500
	total := 0

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		batch := records[i:end]

		var b strings.Builder
		args := make([]any, 0, len(batch)*8)

		b.WriteString(`INSERT INTO analytics.demographics (asset_id, platform_id, territory, demo_date, age_bucket, gender, streams, listeners, updated_at) VALUES `)

		for j, d := range batch {
			if j > 0 {
				b.WriteString(",")
			}
			offset := j * 8
			fmt.Fprintf(&b, "($%d::uuid, $%d::text, $%d::char(2), $%d::date, $%d::text, $%d::text, $%d, $%d, NOW())",
				offset+1, offset+2, offset+3, offset+4, offset+5, offset+6, offset+7, offset+8)
			args = append(args, d.AssetID, d.PlatformID, d.Territory, d.DemoDate, d.AgeBucket, d.Gender, d.Streams, d.Listeners)
		}

		b.WriteString(` ON CONFLICT (asset_id, platform_id, territory, demo_date, age_bucket, gender) DO UPDATE SET
			streams = EXCLUDED.streams, listeners = EXCLUDED.listeners, updated_at = NOW()`)

		tag, err := r.pool.Exec(ctx, b.String(), args...)
		if err != nil {
			return total, fmt.Errorf("bulk upsert demographics: %w", err)
		}
		total += int(tag.RowsAffected())
	}

	return total, nil
}

// SourceQuery holds filters for source breakdown queries.
type SourceQuery struct {
	AssetID   *uuid.UUID
	ClientID  *uuid.UUID
	Platforms []string
	StartDate time.Time
	EndDate   time.Time
}

// QuerySourceBreakdown returns stream counts grouped by source.
func (r *EngagementRepo) QuerySourceBreakdown(ctx context.Context, q SourceQuery) ([]model.SourceBreakdown, error) {
	var b strings.Builder
	args := make([]any, 0, 6)
	argIdx := 1

	b.WriteString(`SELECT e.source, SUM(e.streams) AS streams, SUM(e.saves) AS saves, SUM(e.skips) AS skips, SUM(e.completions) AS completions, SUM(e.discovery) AS discovery
		FROM analytics.engagement e`)

	if q.ClientID != nil {
		fmt.Fprintf(&b, ` JOIN analytics.client_assets ca ON ca.asset_id = e.asset_id AND ca.client_id = $%d`, argIdx)
		args = append(args, *q.ClientID)
		argIdx++
	}

	fmt.Fprintf(&b, ` WHERE e.engagement_date >= $%d AND e.engagement_date <= $%d`, argIdx, argIdx+1)
	args = append(args, q.StartDate, q.EndDate)
	argIdx += 2

	if q.AssetID != nil {
		fmt.Fprintf(&b, ` AND e.asset_id = $%d`, argIdx)
		args = append(args, *q.AssetID)
		argIdx++
	}

	if len(q.Platforms) > 0 {
		fmt.Fprintf(&b, ` AND e.platform_id = ANY($%d)`, argIdx)
		args = append(args, q.Platforms)
		argIdx++
	}

	b.WriteString(` GROUP BY e.source ORDER BY streams DESC`)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query source breakdown: %w", err)
	}
	defer rows.Close()

	var results []model.SourceBreakdown
	var totalStreams int64
	for rows.Next() {
		var sb model.SourceBreakdown
		if err := rows.Scan(&sb.Source, &sb.Streams, &sb.Saves, &sb.Skips, &sb.Completions, &sb.Discovery); err != nil {
			return nil, fmt.Errorf("scan source breakdown: %w", err)
		}
		totalStreams += sb.Streams
		results = append(results, sb)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Calculate percentages
	for i := range results {
		if totalStreams > 0 {
			results[i].Percentage = float64(results[i].Streams) / float64(totalStreams) * 100
		}
	}

	return results, nil
}

// QueryEngagementRates returns aggregated engagement rates for an asset.
func (r *EngagementRepo) QueryEngagementRates(ctx context.Context, assetID uuid.UUID, startDate, endDate time.Time) (*model.EngagementRate, error) {
	var rate model.EngagementRate
	rate.AssetID = assetID

	err := r.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(e.streams), 0), COALESCE(SUM(e.saves), 0), COALESCE(SUM(e.skips), 0), COALESCE(SUM(e.completions), 0)
		 FROM analytics.engagement e
		 WHERE e.asset_id = $1 AND e.engagement_date >= $2 AND e.engagement_date <= $3`,
		assetID, startDate, endDate,
	).Scan(&rate.TotalStreams, &rate.TotalSaves, &rate.TotalSkips, &rate.TotalCompletions)
	if err != nil {
		return nil, fmt.Errorf("query engagement rates: %w", err)
	}

	if rate.TotalStreams > 0 {
		rate.SaveRate = float64(rate.TotalSaves) / float64(rate.TotalStreams)
		rate.SkipRate = float64(rate.TotalSkips) / float64(rate.TotalStreams)
		rate.CompletionRate = float64(rate.TotalCompletions) / float64(rate.TotalStreams)
	}

	return &rate, nil
}

// QueryDiscoveryTrend returns discovery counts over time.
func (r *EngagementRepo) QueryDiscoveryTrend(ctx context.Context, assetID *uuid.UUID, clientID *uuid.UUID, startDate, endDate time.Time) ([]model.DiscoveryPoint, error) {
	var b strings.Builder
	args := make([]any, 0, 4)
	argIdx := 1

	b.WriteString(`SELECT e.engagement_date::text, SUM(e.discovery) AS discovery, SUM(e.streams) AS streams
		FROM analytics.engagement e`)

	if clientID != nil {
		fmt.Fprintf(&b, ` JOIN analytics.client_assets ca ON ca.asset_id = e.asset_id AND ca.client_id = $%d`, argIdx)
		args = append(args, *clientID)
		argIdx++
	}

	fmt.Fprintf(&b, ` WHERE e.engagement_date >= $%d AND e.engagement_date <= $%d`, argIdx, argIdx+1)
	args = append(args, startDate, endDate)
	argIdx += 2

	if assetID != nil {
		fmt.Fprintf(&b, ` AND e.asset_id = $%d`, argIdx)
		args = append(args, *assetID)
		argIdx++
	}

	b.WriteString(` GROUP BY e.engagement_date ORDER BY e.engagement_date`)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query discovery trend: %w", err)
	}
	defer rows.Close()

	var results []model.DiscoveryPoint
	for rows.Next() {
		var dp model.DiscoveryPoint
		if err := rows.Scan(&dp.Date, &dp.Discovery, &dp.Streams); err != nil {
			return nil, fmt.Errorf("scan discovery point: %w", err)
		}
		results = append(results, dp)
	}

	return results, rows.Err()
}

// QueryDemographics returns age/gender breakdown.
func (r *EngagementRepo) QueryDemographics(ctx context.Context, assetID *uuid.UUID, clientID *uuid.UUID, platform *string, startDate, endDate time.Time) ([]model.DemographicBreakdown, error) {
	var b strings.Builder
	args := make([]any, 0, 6)
	argIdx := 1

	b.WriteString(`SELECT d.age_bucket, d.gender, SUM(d.streams) AS streams, SUM(d.listeners) AS listeners
		FROM analytics.demographics d`)

	if clientID != nil {
		fmt.Fprintf(&b, ` JOIN analytics.client_assets ca ON ca.asset_id = d.asset_id AND ca.client_id = $%d`, argIdx)
		args = append(args, *clientID)
		argIdx++
	}

	fmt.Fprintf(&b, ` WHERE d.demo_date >= $%d AND d.demo_date <= $%d`, argIdx, argIdx+1)
	args = append(args, startDate, endDate)
	argIdx += 2

	if assetID != nil {
		fmt.Fprintf(&b, ` AND d.asset_id = $%d`, argIdx)
		args = append(args, *assetID)
		argIdx++
	}

	if platform != nil {
		fmt.Fprintf(&b, ` AND d.platform_id = $%d`, argIdx)
		args = append(args, *platform)
		argIdx++
	}

	b.WriteString(` GROUP BY d.age_bucket, d.gender ORDER BY streams DESC`)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query demographics: %w", err)
	}
	defer rows.Close()

	var results []model.DemographicBreakdown
	var totalStreams int64
	for rows.Next() {
		var db model.DemographicBreakdown
		if err := rows.Scan(&db.AgeBucket, &db.Gender, &db.Streams, &db.Listeners); err != nil {
			return nil, fmt.Errorf("scan demographic breakdown: %w", err)
		}
		totalStreams += db.Streams
		results = append(results, db)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		if totalStreams > 0 {
			results[i].Percentage = float64(results[i].Streams) / float64(totalStreams) * 100
		}
	}

	return results, nil
}
