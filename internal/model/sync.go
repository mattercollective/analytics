package model

import (
	"time"

	"github.com/google/uuid"
)

// SyncState tracks incremental sync progress per platform.
type SyncState struct {
	ID               int       `json:"id"`
	PlatformID       string    `json:"platform_id"`
	Scope            string    `json:"scope"`
	LastSyncAt       *time.Time `json:"last_sync_at,omitempty"`
	LastDataDate     *time.Time `json:"last_data_date,omitempty"`
	CursorToken      *string   `json:"cursor_token,omitempty"`
	BackfillStart    *time.Time `json:"backfill_start,omitempty"`
	BackfillComplete bool      `json:"backfill_complete"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// SyncRun is an audit record for a single sync execution.
type SyncRun struct {
	ID             uuid.UUID  `json:"id"`
	PlatformID     string     `json:"platform_id"`
	Scope          string     `json:"scope"`
	StartedAt      time.Time  `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	Status         string     `json:"status"`
	RowsFetched    int        `json:"rows_fetched"`
	RowsInserted   int        `json:"rows_inserted"`
	RowsUpdated    int        `json:"rows_updated"`
	DataDateMin    *time.Time `json:"data_date_min,omitempty"`
	DataDateMax    *time.Time `json:"data_date_max,omitempty"`
	ErrorMessage   *string    `json:"error_message,omitempty"`
	ErrorCount     int        `json:"error_count"`
	APICallsMade   int        `json:"api_calls_made"`
	RateLimitWaits int        `json:"rate_limit_waits"`
}

// SyncRunUpdate holds mutable fields for updating a sync run.
type SyncRunUpdate struct {
	Status         string
	RowsFetched    int
	RowsInserted   int
	RowsUpdated    int
	DataDateMin    *time.Time
	DataDateMax    *time.Time
	ErrorMessage   *string
	ErrorCount     int
	APICallsMade   int
	RateLimitWaits int
}
