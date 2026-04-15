package model

import (
	"time"

	"github.com/google/uuid"
)

// RevenueType represents the category of revenue.
type RevenueType string

const (
	RevenueAuction          RevenueType = "auction"
	RevenueReserved         RevenueType = "reserved"
	RevenueUGC              RevenueType = "ugc"
	RevenueProRata          RevenueType = "pro_rata"
	RevenueArtTrack         RevenueType = "art_track"
	RevenueShorts           RevenueType = "shorts"
	RevenueAudioTier        RevenueType = "audio_tier"
	RevenueSubscription     RevenueType = "subscription"
	RevenueDownloadPurchase RevenueType = "download_purchase"
	RevenueAdSupported      RevenueType = "ad_supported"
	RevenueOther            RevenueType = "other"
)

// RevenueSource is a high-level revenue categorization.
type RevenueSource string

const (
	RevenueSourceAds           RevenueSource = "ads"
	RevenueSourceSubscriptions RevenueSource = "subscriptions"
	RevenueSourceTransactions  RevenueSource = "transactions"
	RevenueSourceOther         RevenueSource = "other"
)

// Revenue is a single revenue data point stored in analytics.revenue.
type Revenue struct {
	ID            int64          `json:"id"`
	AssetID       uuid.UUID      `json:"asset_id"`
	PlatformID    string         `json:"platform_id"`
	Territory     *string        `json:"territory,omitempty"`
	RevenueDate   time.Time      `json:"revenue_date"`
	RevenueType   RevenueType    `json:"revenue_type"`
	RevenueSource *RevenueSource `json:"revenue_source,omitempty"`
	Currency      string         `json:"currency"`
	Amount        float64        `json:"amount"`
	AmountUSD     *float64       `json:"amount_usd,omitempty"`
	ContentType   *string        `json:"content_type,omitempty"`
	ExternalID    *string        `json:"external_id,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

// RevenueUpsert is the input for upserting a revenue row.
type RevenueUpsert struct {
	AssetID       uuid.UUID
	PlatformID    string
	Territory     *string
	RevenueDate   time.Time
	RevenueType   RevenueType
	RevenueSource *RevenueSource
	Currency      string
	Amount        float64
	AmountUSD     *float64
	ContentType   *string
	ExternalID    *string
}

// RevenueSummary is the response shape for the revenue/summary endpoint.
type RevenueSummary struct {
	AssetID     *uuid.UUID          `json:"asset_id,omitempty"`
	ClientID    *uuid.UUID          `json:"client_id,omitempty"`
	Period      DateRange           `json:"period"`
	Granularity string              `json:"granularity"`
	TotalUSD    float64             `json:"total_usd"`
	Series      []RevenueSummaryRow `json:"series"`
}

type RevenueSummaryRow struct {
	Date       string             `json:"date"`
	TotalUSD   float64            `json:"total_usd"`
	ByPlatform map[string]float64 `json:"by_platform,omitempty"`
}

// RevenueBySource is the response for revenue/by-source.
type RevenueBySource struct {
	Period  DateRange                   `json:"period"`
	Sources []RevenueSourceBreakdown    `json:"sources"`
}

type RevenueSourceBreakdown struct {
	Source    string             `json:"source"`
	TotalUSD float64            `json:"total_usd"`
	ByType   map[string]float64 `json:"by_type,omitempty"`
}

// RevenueByTerritory is the response for revenue/by-territory.
type RevenueByTerritoryRow struct {
	Territory string  `json:"territory"`
	TotalUSD  float64 `json:"total_usd"`
}

// RevenueByPlatform is the response for revenue/by-platform.
type RevenueByPlatformRow struct {
	PlatformID  string  `json:"platform_id"`
	DisplayName string  `json:"display_name"`
	TotalUSD    float64 `json:"total_usd"`
}
