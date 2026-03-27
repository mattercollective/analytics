package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mattercollective/analytics-engine/internal/model"
)

type MetricsRepo struct {
	pool *pgxpool.Pool
}

func NewMetricsRepo(pool *pgxpool.Pool) *MetricsRepo {
	return &MetricsRepo{pool: pool}
}

// BulkUpsert inserts or updates a batch of metrics using a single query.
// Returns (inserted, updated) counts.
func (r *MetricsRepo) BulkUpsert(ctx context.Context, metrics []model.MetricUpsert) (int, int, error) {
	if len(metrics) == 0 {
		return 0, 0, nil
	}

	const batchSize = 500
	var totalInserted, totalUpdated int

	for i := 0; i < len(metrics); i += batchSize {
		end := i + batchSize
		if end > len(metrics) {
			end = len(metrics)
		}
		batch := metrics[i:end]

		ins, upd, err := r.upsertBatch(ctx, batch)
		if err != nil {
			return totalInserted, totalUpdated, err
		}
		totalInserted += ins
		totalUpdated += upd
	}

	return totalInserted, totalUpdated, nil
}

func (r *MetricsRepo) upsertBatch(ctx context.Context, batch []model.MetricUpsert) (int, int, error) {
	var b strings.Builder
	args := make([]any, 0, len(batch)*7)

	b.WriteString(`
		WITH input_rows(asset_id, platform_id, territory, metric_date, metric_type, value, value_decimal, external_id) AS (
			VALUES `)

	for i, m := range batch {
		if i > 0 {
			b.WriteString(",")
		}
		offset := i * 8
		fmt.Fprintf(&b, "($%d::uuid, $%d::text, $%d::char(2), $%d::date, $%d::analytics.metric_type, $%d::bigint, $%d::numeric(18,6), $%d::text)",
			offset+1, offset+2, offset+3, offset+4, offset+5, offset+6, offset+7, offset+8)

		args = append(args, m.AssetID, m.PlatformID, m.Territory, m.MetricDate, string(m.MetricType), m.Value, m.ValueDecimal, m.ExternalID)
	}

	b.WriteString(`
		),
		upserted AS (
			INSERT INTO analytics.metrics (asset_id, platform_id, territory, metric_date, metric_type, value, value_decimal, external_id, updated_at)
			SELECT asset_id, platform_id, territory, metric_date, metric_type, value, value_decimal, external_id, NOW()
			FROM input_rows
			ON CONFLICT (asset_id, platform_id, territory, metric_date, metric_type)
			DO UPDATE SET
				value = EXCLUDED.value,
				value_decimal = EXCLUDED.value_decimal,
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
		return 0, 0, fmt.Errorf("bulk upsert metrics: %w", err)
	}

	return inserted, updated, nil
}

// QuerySummary returns aggregated metrics for the analytics/summary endpoint.
type SummaryQuery struct {
	AssetID     *uuid.UUID
	ClientID    *uuid.UUID
	Platforms   []string
	Metrics     []model.MetricType
	Territories []string
	StartDate   time.Time
	EndDate     time.Time
	Granularity string // "daily", "weekly", "monthly"
}

func (r *MetricsRepo) QuerySummary(ctx context.Context, q SummaryQuery) ([]SummaryRow, error) {
	var dateExpr string
	switch q.Granularity {
	case "weekly":
		dateExpr = "date_trunc('week', m.metric_date)::date"
	case "monthly":
		dateExpr = "date_trunc('month', m.metric_date)::date"
	default:
		dateExpr = "m.metric_date"
	}

	var b strings.Builder
	args := make([]any, 0, 10)
	argIdx := 1

	fmt.Fprintf(&b, `SELECT %s AS period_date, m.platform_id, m.metric_type, SUM(m.value) AS total_value
		FROM analytics.metrics m`, dateExpr)

	if q.ClientID != nil {
		fmt.Fprintf(&b, ` JOIN analytics.client_assets ca ON ca.asset_id = m.asset_id AND ca.client_id = $%d`, argIdx)
		args = append(args, *q.ClientID)
		argIdx++
	}

	fmt.Fprintf(&b, ` WHERE m.metric_date >= $%d AND m.metric_date <= $%d`, argIdx, argIdx+1)
	args = append(args, q.StartDate, q.EndDate)
	argIdx += 2

	if q.AssetID != nil {
		fmt.Fprintf(&b, ` AND m.asset_id = $%d`, argIdx)
		args = append(args, *q.AssetID)
		argIdx++
	}

	if len(q.Platforms) > 0 {
		fmt.Fprintf(&b, ` AND m.platform_id = ANY($%d)`, argIdx)
		args = append(args, q.Platforms)
		argIdx++
	}

	if len(q.Metrics) > 0 {
		metricStrings := make([]string, len(q.Metrics))
		for i, mt := range q.Metrics {
			metricStrings[i] = string(mt)
		}
		fmt.Fprintf(&b, ` AND m.metric_type = ANY($%d::analytics.metric_type[])`, argIdx)
		args = append(args, metricStrings)
		argIdx++
	}

	if len(q.Territories) > 0 {
		fmt.Fprintf(&b, ` AND m.territory = ANY($%d)`, argIdx)
		args = append(args, q.Territories)
		argIdx++
	}

	fmt.Fprintf(&b, ` GROUP BY period_date, m.platform_id, m.metric_type ORDER BY period_date`)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query summary: %w", err)
	}
	defer rows.Close()

	var results []SummaryRow
	for rows.Next() {
		var row SummaryRow
		if err := rows.Scan(&row.PeriodDate, &row.PlatformID, &row.MetricType, &row.TotalValue); err != nil {
			return nil, fmt.Errorf("scan summary row: %w", err)
		}
		results = append(results, row)
	}

	return results, rows.Err()
}

type SummaryRow struct {
	PeriodDate time.Time
	PlatformID string
	MetricType model.MetricType
	TotalValue int64
}

// QueryTopAssets returns the top assets ranked by a given metric.
func (r *MetricsRepo) QueryTopAssets(ctx context.Context, clientID uuid.UUID, metricType model.MetricType, platformID *string, startDate, endDate time.Time, limit int) ([]model.TopAsset, error) {
	var b strings.Builder
	args := make([]any, 0, 6)
	argIdx := 1

	fmt.Fprintf(&b, `SELECT a.id, a.title, a.asset_type, ci.isrc, SUM(m.value) AS total
		FROM analytics.metrics m
		JOIN public.assets a ON a.id = m.asset_id
		JOIN analytics.client_assets ca ON ca.asset_id = m.asset_id AND ca.client_id = $%d
		LEFT JOIN LATERAL (
			SELECT ai.identifier_value AS isrc
			FROM public.asset_identifiers ai
			WHERE ai.asset_id = a.id AND ai.identifier_type = 'isrc' AND ai.effective_to IS NULL
			LIMIT 1
		) ci ON true
		WHERE m.metric_type = $%d::analytics.metric_type
		AND m.metric_date >= $%d AND m.metric_date <= $%d`, argIdx, argIdx+1, argIdx+2, argIdx+3)
	args = append(args, clientID, string(metricType), startDate, endDate)
	argIdx += 4

	if platformID != nil {
		fmt.Fprintf(&b, ` AND m.platform_id = $%d`, argIdx)
		args = append(args, *platformID)
		argIdx++
	}

	fmt.Fprintf(&b, ` GROUP BY a.id, a.title, a.asset_type, ci.isrc ORDER BY total DESC LIMIT $%d`, argIdx)
	args = append(args, limit)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query top assets: %w", err)
	}
	defer rows.Close()

	var results []model.TopAsset
	for rows.Next() {
		var ta model.TopAsset
		ta.MetricType = metricType
		if err := rows.Scan(&ta.AssetID, &ta.Title, &ta.AssetType, &ta.ISRC, &ta.Value); err != nil {
			return nil, fmt.Errorf("scan top asset: %w", err)
		}
		results = append(results, ta)
	}

	return results, rows.Err()
}

// QueryTerritories returns a territory breakdown for an asset/platform/metric.
func (r *MetricsRepo) QueryTerritories(ctx context.Context, assetID uuid.UUID, platformID *string, metricType model.MetricType, startDate, endDate time.Time, limit int) ([]model.TerritoryBreakdown, error) {
	var b strings.Builder
	args := []any{assetID, string(metricType), startDate, endDate}
	argIdx := 5

	b.WriteString(`SELECT m.territory, SUM(m.value) AS total
		FROM analytics.metrics m
		WHERE m.asset_id = $1
		AND m.metric_type = $2::analytics.metric_type
		AND m.metric_date >= $3 AND m.metric_date <= $4
		AND m.territory IS NOT NULL`)

	if platformID != nil {
		fmt.Fprintf(&b, ` AND m.platform_id = $%d`, argIdx)
		args = append(args, *platformID)
		argIdx++
	}

	fmt.Fprintf(&b, ` GROUP BY m.territory ORDER BY total DESC LIMIT $%d`, argIdx)
	args = append(args, limit)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query territories: %w", err)
	}
	defer rows.Close()

	var results []model.TerritoryBreakdown
	for rows.Next() {
		var tb model.TerritoryBreakdown
		if err := rows.Scan(&tb.Territory, &tb.Value); err != nil {
			return nil, fmt.Errorf("scan territory: %w", err)
		}
		results = append(results, tb)
	}

	return results, rows.Err()
}

// ResolveAssetID looks up an asset_id from an identifier (ISRC, UPC, or YT Asset ID).
func (r *MetricsRepo) ResolveAssetID(ctx context.Context, identifierType, identifierValue string) (*uuid.UUID, error) {
	var assetID uuid.UUID
	err := r.pool.QueryRow(ctx,
		`SELECT asset_id FROM public.asset_identifiers
		 WHERE identifier_type = $1::identifier_type AND identifier_value = $2 AND effective_to IS NULL
		 LIMIT 1`,
		identifierType, identifierValue,
	).Scan(&assetID)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resolve asset ID: %w", err)
	}
	return &assetID, nil
}
