package amazon

import (
	"context"
	"fmt"
	"time"

	"github.com/mattercollective/analytics-engine/internal/platform"
)

// Client implements platform.Fetcher for Amazon Music.
// Amazon's API is in closed beta — this is a placeholder adapter
// that will be filled in once API access is granted.
type Client struct {
	apiKey    string
	apiSecret string
}

func NewClient(apiKey, apiSecret string) *Client {
	return &Client{apiKey: apiKey, apiSecret: apiSecret}
}

func (c *Client) Platform() string {
	return "amazon_music"
}

func (c *Client) FetchSince(ctx context.Context, since time.Time, cursor string) (platform.FetchResult, error) {
	// TODO: Implement once Amazon Music API access is granted.
	// Amazon's distributor API is in closed beta.
	// Expected data: daily streaming counts, demographics.
	return platform.FetchResult{}, fmt.Errorf("amazon adapter not yet implemented: API in closed beta")
}
