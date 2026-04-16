package apple

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"golang.org/x/time/rate"

	"github.com/mattercollective/analytics-engine/internal/platform"
)

const analyticsBaseURL = "https://musicanalytics.apple.com/v4/queries/audience-engagement"

// Client implements platform.Fetcher for Apple Music Analytics API v4.
type Client struct {
	httpClient *http.Client
}

func NewClient(teamID, keyID, privateKeyPEM string) (*Client, error) {
	httpClient, err := NewHTTPClient(teamID, keyID, privateKeyPEM)
	if err != nil {
		return nil, err
	}

	// Apple: 3600 requests/hour = 1/sec
	limiter := rate.NewLimiter(1, 5)
	httpClient.Transport = platform.NewRateLimitedTransport(httpClient.Transport, limiter)

	return &Client{httpClient: httpClient}, nil
}

func (c *Client) Platform() string {
	return "apple_music"
}

// FetchSince queries audience-engagement for plays and listeners by day.
func (c *Client) FetchSince(ctx context.Context, since time.Time, cursor string) (platform.FetchResult, error) {
	endDate := time.Now().UTC().Truncate(24 * time.Hour)

	// Build the audience-engagement query
	query := audienceEngagementQuery{
		StartDate: since.Format("2006-01-02"),
		EndDate:   endDate.Format("2006-01-02"),
		GroupBy:   []string{"song_id", "date", "storefront"},
		Metrics:   []string{"plays", "listeners"},
	}

	body, err := json.Marshal(query)
	if err != nil {
		return platform.FetchResult{}, fmt.Errorf("marshal query: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, analyticsBaseURL, bytes.NewReader(body))
	if err != nil {
		return platform.FetchResult{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return platform.FetchResult{}, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return platform.FetchResult{}, fmt.Errorf("apple API error %d: %s", resp.StatusCode, string(respBody))
	}

	var apiResp audienceEngagementResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return platform.FetchResult{}, fmt.Errorf("decode response: %w", err)
	}

	return transformEngagementResponse(apiResp), nil
}

// audienceEngagementQuery is the POST body for the audience-engagement endpoint.
type audienceEngagementQuery struct {
	StartDate string   `json:"start_date"`
	EndDate   string   `json:"end_date"`
	GroupBy   []string `json:"group_by"`
	Metrics   []string `json:"metrics"`
}

// audienceEngagementResponse is the API response shape.
type audienceEngagementResponse struct {
	Results []engagementResult `json:"results"`
	Next    *string            `json:"next"`
}

type engagementResult struct {
	SongID     string `json:"song_id"`
	ISRC       string `json:"isrc"`
	Date       string `json:"date"`
	Storefront string `json:"storefront"`
	Plays      int64  `json:"plays"`
	Listeners  int64  `json:"listeners"`
}
