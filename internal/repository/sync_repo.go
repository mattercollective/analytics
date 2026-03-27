package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mattercollective/analytics-engine/internal/model"
)

type SyncRepo struct {
	pool *pgxpool.Pool
}

func NewSyncRepo(pool *pgxpool.Pool) *SyncRepo {
	return &SyncRepo{pool: pool}
}

// GetSyncState returns the sync state for a platform+scope, or nil if none exists.
func (r *SyncRepo) GetSyncState(ctx context.Context, platformID, scope string) (*model.SyncState, error) {
	var s model.SyncState
	err := r.pool.QueryRow(ctx,
		`SELECT id, platform_id, scope, last_sync_at, last_data_date, cursor_token,
		        backfill_start, backfill_complete, created_at, updated_at
		 FROM analytics.sync_state
		 WHERE platform_id = $1 AND scope = $2`,
		platformID, scope,
	).Scan(&s.ID, &s.PlatformID, &s.Scope, &s.LastSyncAt, &s.LastDataDate,
		&s.CursorToken, &s.BackfillStart, &s.BackfillComplete, &s.CreatedAt, &s.UpdatedAt)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get sync state: %w", err)
	}
	return &s, nil
}

// UpsertSyncState creates or updates the sync state for a platform.
func (r *SyncRepo) UpsertSyncState(ctx context.Context, platformID, scope string, lastSyncAt time.Time, lastDataDate *time.Time, cursorToken *string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO analytics.sync_state (platform_id, scope, last_sync_at, last_data_date, cursor_token, updated_at)
		 VALUES ($1, $2, $3, $4, $5, NOW())
		 ON CONFLICT (platform_id, scope) DO UPDATE SET
		     last_sync_at = EXCLUDED.last_sync_at,
		     last_data_date = COALESCE(EXCLUDED.last_data_date, analytics.sync_state.last_data_date),
		     cursor_token = EXCLUDED.cursor_token,
		     updated_at = NOW()`,
		platformID, scope, lastSyncAt, lastDataDate, cursorToken)
	if err != nil {
		return fmt.Errorf("upsert sync state: %w", err)
	}
	return nil
}

// GetAllSyncStates returns all sync states (for the status endpoint).
func (r *SyncRepo) GetAllSyncStates(ctx context.Context) ([]model.SyncState, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, platform_id, scope, last_sync_at, last_data_date, cursor_token,
		        backfill_start, backfill_complete, created_at, updated_at
		 FROM analytics.sync_state
		 ORDER BY platform_id`)
	if err != nil {
		return nil, fmt.Errorf("get all sync states: %w", err)
	}
	defer rows.Close()

	var results []model.SyncState
	for rows.Next() {
		var s model.SyncState
		if err := rows.Scan(&s.ID, &s.PlatformID, &s.Scope, &s.LastSyncAt, &s.LastDataDate,
			&s.CursorToken, &s.BackfillStart, &s.BackfillComplete, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan sync state: %w", err)
		}
		results = append(results, s)
	}
	return results, rows.Err()
}

// CreateSyncRun starts a new sync run record.
func (r *SyncRepo) CreateSyncRun(ctx context.Context, platformID, scope string) (uuid.UUID, error) {
	var id uuid.UUID
	err := r.pool.QueryRow(ctx,
		`INSERT INTO analytics.sync_runs (platform_id, scope)
		 VALUES ($1, $2)
		 RETURNING id`,
		platformID, scope,
	).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("create sync run: %w", err)
	}
	return id, nil
}

// CompleteSyncRun updates a sync run with final status.
func (r *SyncRepo) CompleteSyncRun(ctx context.Context, id uuid.UUID, update model.SyncRunUpdate) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE analytics.sync_runs SET
		     completed_at = NOW(),
		     status = $2,
		     rows_fetched = $3,
		     rows_inserted = $4,
		     rows_updated = $5,
		     data_date_min = $6,
		     data_date_max = $7,
		     error_message = $8,
		     error_count = $9,
		     api_calls_made = $10,
		     rate_limit_waits = $11
		 WHERE id = $1`,
		id, update.Status, update.RowsFetched, update.RowsInserted, update.RowsUpdated,
		update.DataDateMin, update.DataDateMax, update.ErrorMessage,
		update.ErrorCount, update.APICallsMade, update.RateLimitWaits)
	if err != nil {
		return fmt.Errorf("complete sync run: %w", err)
	}
	return nil
}

// ListSyncRuns returns recent sync runs, optionally filtered by platform.
func (r *SyncRepo) ListSyncRuns(ctx context.Context, platformID *string, limit int) ([]model.SyncRun, error) {
	var query string
	var args []any

	if platformID != nil {
		query = `SELECT id, platform_id, scope, started_at, completed_at, status,
		                rows_fetched, rows_inserted, rows_updated, data_date_min, data_date_max,
		                error_message, error_count, api_calls_made, rate_limit_waits
		         FROM analytics.sync_runs
		         WHERE platform_id = $1
		         ORDER BY started_at DESC LIMIT $2`
		args = []any{*platformID, limit}
	} else {
		query = `SELECT id, platform_id, scope, started_at, completed_at, status,
		                rows_fetched, rows_inserted, rows_updated, data_date_min, data_date_max,
		                error_message, error_count, api_calls_made, rate_limit_waits
		         FROM analytics.sync_runs
		         ORDER BY started_at DESC LIMIT $1`
		args = []any{limit}
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list sync runs: %w", err)
	}
	defer rows.Close()

	var results []model.SyncRun
	for rows.Next() {
		var sr model.SyncRun
		if err := rows.Scan(&sr.ID, &sr.PlatformID, &sr.Scope, &sr.StartedAt, &sr.CompletedAt,
			&sr.Status, &sr.RowsFetched, &sr.RowsInserted, &sr.RowsUpdated,
			&sr.DataDateMin, &sr.DataDateMax, &sr.ErrorMessage, &sr.ErrorCount,
			&sr.APICallsMade, &sr.RateLimitWaits); err != nil {
			return nil, fmt.Errorf("scan sync run: %w", err)
		}
		results = append(results, sr)
	}
	return results, rows.Err()
}
