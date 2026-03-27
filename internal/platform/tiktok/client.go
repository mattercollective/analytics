package tiktok

import (
	"context"
	"fmt"
	"time"

	"github.com/mattercollective/analytics-engine/internal/platform"
)

// Client implements platform.Fetcher for TikTok.
// No official distributor API exists. This adapter serves as a placeholder.
// For now, TikTok data comes via CSV import through the API endpoint.
type Client struct{}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) Platform() string {
	return "tiktok"
}

func (c *Client) FetchSince(ctx context.Context, since time.Time, cursor string) (platform.FetchResult, error) {
	// TikTok has no official distributor analytics API.
	// Data should be imported via CSV upload at POST /api/v1/sync/tiktok/upload
	return platform.FetchResult{}, fmt.Errorf("tiktok does not support API fetch: use CSV upload endpoint instead")
}
