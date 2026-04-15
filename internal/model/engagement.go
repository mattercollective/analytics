package model

import (
	"time"

	"github.com/google/uuid"
)

// Engagement is a single source-level engagement row.
type Engagement struct {
	ID             int64     `json:"id"`
	AssetID        uuid.UUID `json:"asset_id"`
	PlatformID     string    `json:"platform_id"`
	Territory      *string   `json:"territory,omitempty"`
	EngagementDate time.Time `json:"engagement_date"`
	Source         string    `json:"source"`
	SourceURI      *string   `json:"source_uri,omitempty"`
	Streams        int64     `json:"streams"`
	Saves          int64     `json:"saves"`
	Skips          int64     `json:"skips"`
	Completions    int64     `json:"completions"`
	Discovery      int64     `json:"discovery"`
}

// EngagementUpsert is the input for upserting engagement data.
type EngagementUpsert struct {
	AssetID        uuid.UUID
	PlatformID     string
	Territory      *string
	EngagementDate time.Time
	Source         string
	SourceURI      *string
	Streams        int64
	Saves          int64
	Skips          int64
	Completions    int64
	Discovery      int64
}

// SourceBreakdown shows streams and engagement by discovery source.
type SourceBreakdown struct {
	Source      string  `json:"source"`
	Streams     int64   `json:"streams"`
	Saves       int64   `json:"saves"`
	Skips       int64   `json:"skips"`
	Completions int64   `json:"completions"`
	Discovery   int64   `json:"discovery"`
	Percentage  float64 `json:"percentage"` // % of total streams from this source
}

// EngagementRate holds derived engagement ratios.
type EngagementRate struct {
	AssetID        uuid.UUID `json:"asset_id"`
	TotalStreams   int64     `json:"total_streams"`
	TotalSaves     int64     `json:"total_saves"`
	TotalSkips     int64     `json:"total_skips"`
	TotalCompletions int64   `json:"total_completions"`
	SaveRate       float64   `json:"save_rate"`       // saves / streams
	SkipRate       float64   `json:"skip_rate"`       // skips / streams
	CompletionRate float64   `json:"completion_rate"` // completions / streams
}

// DiscoveryPoint is one point in a discovery trend time-series.
type DiscoveryPoint struct {
	Date      string `json:"date"`
	Discovery int64  `json:"discovery"`
	Streams   int64  `json:"streams"`
}

// DemographicUpsert is the input for upserting demographic data.
type DemographicUpsert struct {
	AssetID    uuid.UUID
	PlatformID string
	Territory  *string
	DemoDate   time.Time
	AgeBucket  string
	Gender     string
	Streams    int64
	Listeners  int64
}

// DemographicBreakdown is a single age/gender bucket.
type DemographicBreakdown struct {
	AgeBucket  string  `json:"age_bucket"`
	Gender     string  `json:"gender"`
	Streams    int64   `json:"streams"`
	Listeners  int64   `json:"listeners"`
	Percentage float64 `json:"percentage"` // % of total
}
