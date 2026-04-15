package model

import (
	"time"

	"github.com/google/uuid"
)

// Playlist represents a playlist entity across any platform.
type Playlist struct {
	ID            uuid.UUID `json:"id"`
	PlatformID    string    `json:"platform_id"`
	ExternalID    string    `json:"external_id"`
	Name          string    `json:"name"`
	Description   *string   `json:"description,omitempty"`
	CuratorName   *string   `json:"curator_name,omitempty"`
	CuratorType   *string   `json:"curator_type,omitempty"`
	FollowerCount *int64    `json:"follower_count,omitempty"`
	TrackCount    *int      `json:"track_count,omitempty"`
	IsActive      bool      `json:"is_active"`
	ImageURL      *string   `json:"image_url,omitempty"`
	PlatformURL   *string   `json:"platform_url,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// PlaylistUpsert is the input for creating or updating a playlist.
type PlaylistUpsert struct {
	PlatformID    string
	ExternalID    string
	Name          string
	Description   *string
	CuratorName   *string
	CuratorType   *string
	FollowerCount *int64
	TrackCount    *int
	ImageURL      *string
	PlatformURL   *string
}

// PlaylistPosition is a single snapshot of a track's position in a playlist.
type PlaylistPosition struct {
	PlaylistID   uuid.UUID
	AssetID      uuid.UUID
	SnapshotDate time.Time
	Position     *int
	AddedDate    *time.Time
}

// PlaylistWithPosition combines playlist metadata with an asset's current position.
type PlaylistWithPosition struct {
	Playlist
	Position  *int    `json:"position,omitempty"`
	AddedDate *string `json:"added_date,omitempty"`
}

// PositionPoint is a single point in a position history time-series.
type PositionPoint struct {
	Date     string `json:"date"`
	Position *int   `json:"position,omitempty"`
	Present  bool   `json:"present"` // false = track was not on the playlist that day
}

// PlaylistAttribution links a playlist to estimated stream contribution.
type PlaylistAttribution struct {
	PlaylistID    uuid.UUID `json:"playlist_id"`
	PlaylistName  string    `json:"playlist_name"`
	PlatformID    string    `json:"platform_id"`
	CuratorType   *string   `json:"curator_type,omitempty"`
	FollowerCount *int64    `json:"follower_count,omitempty"`
	AssetCount    int       `json:"asset_count"`    // how many of your tracks are on this playlist
	TotalStreams  int64     `json:"total_streams"`   // estimated stream contribution
}
