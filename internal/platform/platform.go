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

// EngagementFetcher is an optional interface for platforms that provide
// source-level engagement and demographic data (e.g., Spotify Bulk API).
type EngagementFetcher interface {
	FetchEngagement(ctx context.Context, since time.Time, cursor string) (EngagementResult, error)
	FetchDemographics(ctx context.Context, since time.Time, cursor string) (DemographicsResult, error)
}

// RawEngagement is a source-level engagement record before asset resolution.
type RawEngagement struct {
	ISRC        string
	Territory   string
	Date        time.Time
	Source      string  // 'playlist', 'radio', 'search', 'album', 'artist_page', etc.
	SourceURI   string  // e.g., Spotify playlist URI
	Streams     int64
	Saves       int64
	Skips       int64
	Completions int64
	Discovery   int64
}

// EngagementResult contains a page of engagement data.
type EngagementResult struct {
	Records    []RawEngagement
	NextCursor string
	HasMore    bool
}

// RawDemographic is an age/gender breakdown record before asset resolution.
type RawDemographic struct {
	ISRC      string
	Territory string
	Date      time.Time
	AgeBucket string // '13-17', '18-24', '25-34', etc.
	Gender    string // 'male', 'female', 'non_binary', 'unknown'
	Streams   int64
	Listeners int64
}

// DemographicsResult contains a page of demographic data.
type DemographicsResult struct {
	Records    []RawDemographic
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
