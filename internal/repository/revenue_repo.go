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

type RevenueRepo struct {
	pool *pgxpool.Pool
}

func NewRevenueRepo(pool *pgxpool.Pool) *RevenueRepo {
	return &RevenueRepo{pool: pool}
}

// BulkUpsert inserts or updates a batch of revenue records.
func (r *RevenueRepo) BulkUpsert(ctx context.Context, revenues []model.RevenueUpsert) (int, int, error) {
	if len(revenues) == 0 {
		return 0, 0, nil
	}

	const batchSize = 500
	var totalInserted, totalUpdated int

	for i := 0; i < len(revenues); i += batchSize {
		end := i + batchSize
		if end > len(revenues) {
			end = len(revenues)
		}
		batch := revenues[i:end]

		ins, upd, err := r.upsertBatch(ctx, batch)
		if err != nil {
			return totalInserted, totalUpdated, err
		}
		totalInserted += ins
		totalUpdated += upd
	}

	return totalInserted, totalUpdated, nil
}

func (r *RevenueRepo) upsertBatch(ctx context.Context, batch []model.RevenueUpsert) (int, int, error) {
	var b strings.Builder
	args := make([]any, 0, len(batch)*11)

	b.WriteString(`
		WITH input_rows(asset_id, platform_id, territory, revenue_date, revenue_type, revenue_source, currency, amount, amount_usd, content_type, external_id) AS (
			VALUES `)

	for i, rv := range batch {
		if i > 0 {
			b.WriteString(",")
		}
		offset := i * 11
		fmt.Fprintf(&b, "($%d::uuid, $%d::text, $%d::char(2), $%d::date, $%d::analytics.revenue_type, $%d::analytics.revenue_source, $%d::char(3), $%d::numeric(18,6), $%d::numeric(18,6), $%d::text, $%d::text)",
			offset+1, offset+2, offset+3, offset+4, offset+5, offset+6, offset+7, offset+8, offset+9, offset+10, offset+11)

		args = append(args, rv.AssetID, rv.PlatformID, rv.Territory, rv.RevenueDate, string(rv.RevenueType), ptrRevenueSourceStr(rv.RevenueSource), rv.Currency, rv.Amount, rv.AmountUSD, rv.ContentType, rv.ExternalID)
	}

	b.WriteString(`
		),
		upserted AS (
			INSERT INTO analytics.revenue (asset_id, platform_id, territory, revenue_date, revenue_type, revenue_source, currency, amount, amount_usd, content_type, external_id, updated_at)
			SELECT asset_id, platform_id, territory, revenue_date, revenue_type, revenue_source, currency, amount, amount_usd, content_type, external_id, NOW()
			FROM input_rows
			ON CONFLICT (asset_id, platform_id, territory, revenue_date, revenue_type)
			DO UPDATE SET
				revenue_source = EXCLUDED.revenue_source,
				currency = EXCLUDED.currency,
				amount = EXCLUDED.amount,
				amount_usd = EXCLUDED.amount_usd,
				content_type = EXCLUDED.content_type,
				external_id = EXCLUDED.external_id,
				updated_at = NOW()
			RETURNING (xmax = 0) AS inserted
		)
		SELECT
			COUNT(*) FILTER (WHERE inserted) AS inserted_count,
			COUNT(*) FILTER (WHERE NOT inserted) AS updated_count
		FROM upserted`)

	var inserted, updated int
	err := r.pool.QueryRow(ctx, b.String(), args...).Scan(&inserted, &updated)
	if err != nil {
		return 0, 0, fmt.Errorf("bulk upsert revenue: %w", err)
	}

	return inserted, updated, nil
}

func ptrRevenueSourceStr(rs *model.RevenueSource) *string {
	if rs == nil {
		return nil
	}
	s := string(*rs)
	return &s
}

// RevenueSummaryQuery holds filters for revenue summary queries.
type RevenueSummaryQuery struct {
	AssetID     *uuid.UUID
	ClientID    *uuid.UUID
	Platforms   []string
	StartDate   time.Time
	EndDate     time.Time
	Granularity string // "daily", "weekly", "monthly"
}

type RevenueSummaryDBRow struct {
	PeriodDate time.Time
	PlatformID string
	TotalUSD   float64
}

func (r *RevenueRepo) QuerySummary(ctx context.Context, q RevenueSummaryQuery) ([]RevenueSummaryDBRow, error) {
	var dateExpr string
	switch q.Granularity {
	case "weekly":
		dateExpr = "date_trunc('week', rv.revenue_date)::date"
	case "monthly":
		dateExpr = "date_trunc('month', rv.revenue_date)::date"
	default:
		dateExpr = "rv.revenue_date"
	}

	var b strings.Builder
	args := make([]any, 0, 10)
	argIdx := 1

	fmt.Fprintf(&b, `SELECT %s AS period_date, rv.platform_id, COALESCE(SUM(rv.amount_usd), SUM(rv.amount)) AS total_usd
		FROM analytics.revenue rv`, dateExpr)

	if q.ClientID != nil {
		fmt.Fprintf(&b, ` JOIN analytics.client_assets ca ON ca.asset_id = rv.asset_id AND ca.client_id = $%d`, argIdx)
		args = append(args, *q.ClientID)
		argIdx++
	}

	fmt.Fprintf(&b, ` WHERE rv.revenue_date >= $%d AND rv.revenue_date <= $%d`, argIdx, argIdx+1)
	args = append(args, q.StartDate, q.EndDate)
	argIdx += 2

	if q.AssetID != nil {
		fmt.Fprintf(&b, ` AND rv.asset_id = $%d`, argIdx)
		args = append(args, *q.AssetID)
		argIdx++
	}

	if len(q.Platforms) > 0 {
		fmt.Fprintf(&b, ` AND rv.platform_id = ANY($%d)`, argIdx)
		args = append(args, q.Platforms)
		argIdx++
	}

	fmt.Fprintf(&b, ` GROUP BY period_date, rv.platform_id ORDER BY period_date`)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query revenue summary: %w", err)
	}
	defer rows.Close()

	var results []RevenueSummaryDBRow
	for rows.Next() {
		var row RevenueSummaryDBRow
		if err := rows.Scan(&row.PeriodDate, &row.PlatformID, &row.TotalUSD); err != nil {
			return nil, fmt.Errorf("scan revenue summary row: %w", err)
		}
		results = append(results, row)
	}

	return results, rows.Err()
}

// RevenueBySourceQuery holds filters for revenue by-source queries.
type RevenueBySourceQuery struct {
	AssetID   *uuid.UUID
	ClientID  *uuid.UUID
	Platforms []string
	StartDate time.Time
	EndDate   time.Time
}

type RevenueBySourceDBRow struct {
	Source   string
	Type     string
	TotalUSD float64
}

func (r *RevenueRepo) QueryBySource(ctx context.Context, q RevenueBySourceQuery) ([]RevenueBySourceDBRow, error) {
	var b strings.Builder
	args := make([]any, 0, 10)
	argIdx := 1

	b.WriteString(`SELECT COALESCE(rv.revenue_source::text, 'other') AS source, rv.revenue_type::text AS type, COALESCE(SUM(rv.amount_usd), SUM(rv.amount)) AS total_usd
		FROM analytics.revenue rv`)

	if q.ClientID != nil {
		fmt.Fprintf(&b, ` JOIN analytics.client_assets ca ON ca.asset_id = rv.asset_id AND ca.client_id = $%d`, argIdx)
		args = append(args, *q.ClientID)
		argIdx++
	}

	fmt.Fprintf(&b, ` WHERE rv.revenue_date >= $%d AND rv.revenue_date <= $%d`, argIdx, argIdx+1)
	args = append(args, q.StartDate, q.EndDate)
	argIdx += 2

	if q.AssetID != nil {
		fmt.Fprintf(&b, ` AND rv.asset_id = $%d`, argIdx)
		args = append(args, *q.AssetID)
		argIdx++
	}

	if len(q.Platforms) > 0 {
		fmt.Fprintf(&b, ` AND rv.platform_id = ANY($%d)`, argIdx)
		args = append(args, q.Platforms)
		argIdx++
	}

	b.WriteString(` GROUP BY source, type ORDER BY total_usd DESC`)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query revenue by source: %w", err)
	}
	defer rows.Close()

	var results []RevenueBySourceDBRow
	for rows.Next() {
		var row RevenueBySourceDBRow
		if err := rows.Scan(&row.Source, &row.Type, &row.TotalUSD); err != nil {
			return nil, fmt.Errorf("scan revenue by source row: %w", err)
		}
		results = append(results, row)
	}

	return results, rows.Err()
}

type RevenueByTerritoryQuery struct {
	AssetID   uuid.UUID
	Platforms []string
	StartDate time.Time
	EndDate   time.Time
	Limit     int
}

func (r *RevenueRepo) QueryByTerritory(ctx context.Context, q RevenueByTerritoryQuery) ([]model.RevenueByTerritoryRow, error) {
	var b strings.Builder
	args := []any{q.AssetID, q.StartDate, q.EndDate}
	argIdx := 4

	b.WriteString(`SELECT rv.territory, COALESCE(SUM(rv.amount_usd), SUM(rv.amount)) AS total_usd
		FROM analytics.revenue rv
		WHERE rv.asset_id = $1
		AND rv.revenue_date >= $2 AND rv.revenue_date <= $3
		AND rv.territory IS NOT NULL`)

	if len(q.Platforms) > 0 {
		fmt.Fprintf(&b, ` AND rv.platform_id = ANY($%d)`, argIdx)
		args = append(args, q.Platforms)
		argIdx++
	}

	fmt.Fprintf(&b, ` GROUP BY rv.territory ORDER BY total_usd DESC LIMIT $%d`, argIdx)
	args = append(args, q.Limit)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query revenue by territory: %w", err)
	}
	defer rows.Close()

	var results []model.RevenueByTerritoryRow
	for rows.Next() {
		var row model.RevenueByTerritoryRow
		if err := rows.Scan(&row.Territory, &row.TotalUSD); err != nil {
			return nil, fmt.Errorf("scan revenue by territory row: %w", err)
		}
		results = append(results, row)
	}

	return results, rows.Err()
}

type RevenueByPlatformQuery struct {
	ClientID  uuid.UUID
	StartDate time.Time
	EndDate   time.Time
}

func (r *RevenueRepo) QueryByPlatform(ctx context.Context, q RevenueByPlatformQuery) ([]model.RevenueByPlatformRow, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT rv.platform_id, p.display_name, COALESCE(SUM(rv.amount_usd), SUM(rv.amount)) AS total_usd
		FROM analytics.revenue rv
		JOIN analytics.client_assets ca ON ca.asset_id = rv.asset_id AND ca.client_id = $1
		JOIN public.platforms p ON p.id = rv.platform_id
		WHERE rv.revenue_date >= $2 AND rv.revenue_date <= $3
		GROUP BY rv.platform_id, p.display_name
		ORDER BY total_usd DESC`,
		q.ClientID, q.StartDate, q.EndDate,
	)
	if err != nil {
		return nil, fmt.Errorf("query revenue by platform: %w", err)
	}
	defer rows.Close()

	var results []model.RevenueByPlatformRow
	for rows.Next() {
		var row model.RevenueByPlatformRow
		if err := rows.Scan(&row.PlatformID, &row.DisplayName, &row.TotalUSD); err != nil {
			return nil, fmt.Errorf("scan revenue by platform row: %w", err)
		}
		results = append(results, row)
	}

	return results, rows.Err()
}
