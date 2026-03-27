package apple

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

const analyticsBaseURL = "https://api.music.apple.com/v1/me/analytics"

// Client implements platform.Fetcher for Apple Music Analytics API.
type Client struct {
	httpClient *http.Client
}

func NewClient(teamID, keyID, privateKeyPEM string) (*Client, error) {
	httpClient, err := NewHTTPClient(teamID, keyID, privateKeyPEM)
	if err != nil {
		return nil, err
	}

	// 20 req/sec rate limit
	limiter := rate.NewLimiter(20, 20)
	httpClient.Transport = platform.NewRateLimitedTransport(httpClient.Transport, limiter)

	return &Client{httpClient: httpClient}, nil
}

func (c *Client) Platform() string {
	return "apple_music"
}

func (c *Client) FetchSince(ctx context.Context, since time.Time, cursor string) (platform.FetchResult, error) {
	u, err := url.Parse(analyticsBaseURL + "/streams")
	if err != nil {
		return platform.FetchResult{}, fmt.Errorf("parse URL: %w", err)
	}

	q := u.Query()
	q.Set("startDate", since.Format("2006-01-02"))
	q.Set("endDate", time.Now().UTC().Format("2006-01-02"))
	if cursor != "" {
		q.Set("offset", cursor)
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
		return platform.FetchResult{}, fmt.Errorf("apple API error %d: %s", resp.StatusCode, string(body))
	}

	var apiResp appleAnalyticsResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return platform.FetchResult{}, fmt.Errorf("decode response: %w", err)
	}

	return transformResponse(apiResp), nil
}

type appleAnalyticsResponse struct {
	Data []appleStreamRecord `json:"data"`
	Next *string             `json:"next"`
}

type appleStreamRecord struct {
	ISRC      string `json:"isrc"`
	Date      string `json:"date"`
	Territory string `json:"storefront"`
	Plays     int64  `json:"plays"`
	Listeners int64  `json:"listeners"`
}
