package platform

import (
	"context"
	"time"
)

// Fetcher is the interface every platform adapter must implement.
type Fetcher interface {
	// Platform returns the platform_id (e.g., "spotify", "youtube").
	Platform() string

	// FetchSince retrieves analytics data since the given time.
	// Pass an empty cursor for the first page. Returns results and
	// an optional cursor for pagination.
	FetchSince(ctx context.Context, since time.Time, cursor string) (FetchResult, error)
}

// FetchResult contains a page of raw metrics and pagination state.
type FetchResult struct {
	Metrics    []RawMetric
	NextCursor string
	HasMore    bool
}

// RawMetric is a platform-agnostic metric before asset resolution.
// At least one identifier (ISRC, UPC, YTAssetID) must be set.
type RawMetric struct {
	ISRC        string
	UPC         string
	YTAssetID   string
	YTChannelID string
	Territory   string // ISO 3166-1 alpha-2
	Date        time.Time
	MetricType  string
	Value       int64
	ValueDecimal *float64
	ExternalID  string            // platform-specific dedup ID
	RawData     map[string]any    // original payload for audit
}
