package model

import (
	"time"

	"github.com/google/uuid"
)

// MetricType represents the kind of engagement metric.
type MetricType string

const (
	MetricStreams         MetricType = "streams"
	MetricViews          MetricType = "views"
	MetricDownloads      MetricType = "downloads"
	MetricSaves          MetricType = "saves"
	MetricPlaylistAdds   MetricType = "playlist_adds"
	MetricPlaylistRemoves MetricType = "playlist_removes"
	MetricFollowers      MetricType = "followers"
	MetricListeners      MetricType = "listeners"
	MetricShazams        MetricType = "shazams"
	MetricSoundUses      MetricType = "sound_uses"
	MetricWatchTimeHours MetricType = "watch_time_hours"
	MetricImpressions    MetricType = "impressions"
	MetricLikes          MetricType = "likes"
	MetricShares         MetricType = "shares"
	MetricComments       MetricType = "comments"
	MetricContentIDClaims MetricType = "content_id_claims"
	MetricRevenueEstimate MetricType = "revenue_estimate"
)

// Metric is a single analytics data point stored in analytics.metrics.
type Metric struct {
	ID           int64      `json:"id"`
	AssetID      uuid.UUID  `json:"asset_id"`
	PlatformID   string     `json:"platform_id"`
	Territory    *string    `json:"territory,omitempty"`
	MetricDate   time.Time  `json:"metric_date"`
	MetricType   MetricType `json:"metric_type"`
	Value        int64      `json:"value"`
	ValueDecimal *float64   `json:"value_decimal,omitempty"`
	ExternalID   *string    `json:"external_id,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

// MetricUpsert is the input for upserting a metric row.
type MetricUpsert struct {
	AssetID      uuid.UUID
	PlatformID   string
	Territory    *string
	MetricDate   time.Time
	MetricType   MetricType
	Value        int64
	ValueDecimal *float64
	ExternalID   *string
}

// AnalyticsSummary is the response shape for the summary endpoint.
type AnalyticsSummary struct {
	AssetID     *uuid.UUID                `json:"asset_id,omitempty"`
	ClientID    *uuid.UUID                `json:"client_id,omitempty"`
	Period      DateRange                 `json:"period"`
	Granularity string                    `json:"granularity"`
	Series      []AnalyticsSummaryPoint   `json:"series"`
	Totals      map[MetricType]int64      `json:"totals"`
}

type DateRange struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

type AnalyticsSummaryPoint struct {
	Date       string                          `json:"date"`
	Metrics    map[MetricType]int64            `json:"metrics"`
	ByPlatform map[string]map[MetricType]int64 `json:"by_platform,omitempty"`
}

// TopAsset ranks an asset by a given metric.
type TopAsset struct {
	AssetID    uuid.UUID  `json:"asset_id"`
	Title      string     `json:"title"`
	AssetType  string     `json:"asset_type"`
	ISRC       *string    `json:"isrc,omitempty"`
	Value      int64      `json:"value"`
	MetricType MetricType `json:"metric_type"`
}

// TerritoryBreakdown shows metric values per territory.
type TerritoryBreakdown struct {
	Territory string `json:"territory"`
	Value     int64  `json:"value"`
}

// PlatformBreakdown shows metric values per platform.
type PlatformBreakdown struct {
	PlatformID   string                   `json:"platform_id"`
	DisplayName  string                   `json:"display_name"`
	Metrics      map[MetricType]int64     `json:"metrics"`
}
