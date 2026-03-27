package model

import (
	"time"

	"github.com/google/uuid"
)

// Asset mirrors the public.assets table (read-only in analytics engine).
type Asset struct {
	ID        uuid.UUID `json:"id"`
	AssetType string    `json:"asset_type"`
	Title     string    `json:"title"`
	IsActive  bool      `json:"is_active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// AssetWithIdentifiers includes current active identifiers.
type AssetWithIdentifiers struct {
	Asset
	ISRC        *string `json:"isrc,omitempty"`
	UPC         *string `json:"upc,omitempty"`
	YTAssetID   *string `json:"yt_asset_id,omitempty"`
	YTChannelID *string `json:"yt_channel_id,omitempty"`
}

// Platform mirrors the public.platforms table.
type Platform struct {
	ID          string  `json:"id"`
	DisplayName string  `json:"display_name"`
	Category    string  `json:"category"`
	IsActive    bool    `json:"is_active"`
}

// PlatformStatus includes sync state for the platforms endpoint.
type PlatformStatus struct {
	Platform
	LastSyncAt    *time.Time `json:"last_sync_at,omitempty"`
	LastDataDate  *string    `json:"last_data_date,omitempty"`
	SyncStatus    *string    `json:"sync_status,omitempty"`
}

// Client mirrors the public.clients table (read-only).
type Client struct {
	ID       uuid.UUID `json:"id"`
	Name     string    `json:"name"`
	IsActive bool      `json:"is_active"`
}
