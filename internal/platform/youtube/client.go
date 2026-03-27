package youtube

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

const analyticsURL = "https://youtubeanalytics.googleapis.com/v2/reports"

// Client implements platform.Fetcher for YouTube Analytics + Content ID.
type Client struct {
	httpClient     *http.Client
	contentOwnerID string
	quota          *platform.QuotaLimiter
}

func NewClient(ctx context.Context, serviceAccountJSON, contentOwnerID string) (*Client, error) {
	httpClient, err := NewAuthenticatedClient(ctx, serviceAccountJSON)
	if err != nil {
		return nil, err
	}

	// YouTube rate limit: gentle pacing to stay within 10K quota/day
	limiter := rate.NewLimiter(rate.Every(2*time.Second), 5)
	httpClient.Transport = platform.NewRateLimitedTransport(httpClient.Transport, limiter)

	return &Client{
		httpClient:     httpClient,
		contentOwnerID: contentOwnerID,
		quota:          platform.NewQuotaLimiter(10000),
	}, nil
}

func (c *Client) Platform() string {
	return "youtube"
}

func (c *Client) FetchSince(ctx context.Context, since time.Time, cursor string) (platform.FetchResult, error) {
	// Each query costs ~1-5 quota units
	if err := c.quota.Use(ctx, 5); err != nil {
		return platform.FetchResult{}, fmt.Errorf("youtube quota exceeded: %w", err)
	}

	u, err := url.Parse(analyticsURL)
	if err != nil {
		return platform.FetchResult{}, fmt.Errorf("parse URL: %w", err)
	}

	q := u.Query()
	q.Set("ids", "contentOwner=="+c.contentOwnerID)
	q.Set("startDate", since.Format("2006-01-02"))
	q.Set("endDate", time.Now().UTC().Format("2006-01-02"))
	q.Set("metrics", "views,estimatedMinutesWatched,likes,shares,comments")
	q.Set("dimensions", "video,day,country")
	q.Set("sort", "day")
	if cursor != "" {
		q.Set("startIndex", cursor)
	}
	q.Set("maxResults", "10000")
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
		return platform.FetchResult{}, fmt.Errorf("youtube API error %d: %s", resp.StatusCode, string(body))
	}

	var apiResp youtubeAnalyticsResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return platform.FetchResult{}, fmt.Errorf("decode response: %w", err)
	}

	return transformResponse(apiResp), nil
}

// youtubeAnalyticsResponse is the YouTube Analytics API response shape.
type youtubeAnalyticsResponse struct {
	ColumnHeaders []struct {
		Name string `json:"name"`
	} `json:"columnHeaders"`
	Rows [][]json.RawMessage `json:"rows"`
}
