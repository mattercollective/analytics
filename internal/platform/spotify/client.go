package spotify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/time/rate"

	"github.com/mattercollective/analytics-engine/internal/platform"
)

const (
	baseURL     = "https://api.spotify.com"
	bulkBaseURL = "https://provider-api.spotify.com" // Enhanced Bulk API base
)

// Client implements platform.Fetcher for the Spotify Enhanced Bulk API.
type Client struct {
	httpClient *http.Client
	limiter    *rate.Limiter
}

// NewClient creates a Spotify fetcher with rate limiting.
func NewClient(ctx context.Context, clientID, clientSecret string) *Client {
	httpClient := NewHTTPClient(ctx, clientID, clientSecret)

	// Spotify rolling 30-second window rate limit
	limiter := rate.NewLimiter(rate.Every(time.Second), 10)

	httpClient.Transport = platform.NewRateLimitedTransport(httpClient.Transport, limiter)

	return &Client{
		httpClient: httpClient,
		limiter:    limiter,
	}
}

func (c *Client) Platform() string {
	return "spotify"
}

// FetchSince retrieves streaming analytics since the given time.
// Uses the Enhanced Bulk API endpoint for distributor-grade data.
func (c *Client) FetchSince(ctx context.Context, since time.Time, cursor string) (platform.FetchResult, error) {
	u, err := url.Parse(bulkBaseURL + "/v1/analytics/streams")
	if err != nil {
		return platform.FetchResult{}, fmt.Errorf("parse URL: %w", err)
	}

	q := u.Query()
	q.Set("start_date", since.Format("2006-01-02"))
	q.Set("end_date", time.Now().UTC().Format("2006-01-02"))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return platform.FetchResult{}, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return platform.FetchResult{}, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return platform.FetchResult{}, fmt.Errorf("spotify API error %d: %s", resp.StatusCode, string(body))
	}

	var apiResp bulkAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return platform.FetchResult{}, fmt.Errorf("decode response: %w", err)
	}

	return transformResponse(apiResp), nil
}

// bulkAPIResponse represents the Spotify Enhanced Bulk API response shape.
// This will need adjustment when we get actual API documentation with approval.
type bulkAPIResponse struct {
	Data []bulkStreamRecord `json:"data"`
	Next *string            `json:"next"`
}

type bulkStreamRecord struct {
	ISRC        string `json:"isrc"`
	Date        string `json:"date"`
	Territory   string `json:"territory"`
	Streams     int64  `json:"streams"`
	Listeners   int64  `json:"listeners"`
	Saves       int64  `json:"saves"`
	PlaylistAdds int64 `json:"playlist_adds"`
}
