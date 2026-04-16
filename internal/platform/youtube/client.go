package youtube

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/time/rate"

	"github.com/mattercollective/analytics-engine/internal/platform"
)

const analyticsURL = "https://youtubeanalytics.googleapis.com/v2/reports"

// Client implements platform.Fetcher and platform.EngagementFetcher
// for YouTube Analytics + Content ID.
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

// FetchSince retrieves views, watch time, likes, shares, comments, and revenue.
func (c *Client) FetchSince(ctx context.Context, since time.Time, cursor string) (platform.FetchResult, error) {
	if err := c.quota.Use(ctx, 10); err != nil {
		return platform.FetchResult{}, fmt.Errorf("youtube quota exceeded: %w", err)
	}

	startIdx := 0
	if cursor != "" {
		startIdx, _ = strconv.Atoi(cursor)
	}

	// Query 1: Basic engagement metrics by day × country (content owner level)
	basicResp, err := c.queryAnalytics(ctx, queryParams{
		startDate:  since,
		endDate:    time.Now().UTC(),
		metrics:    "views,estimatedMinutesWatched,likes,shares,comments",
		dimensions: "day,country",
		sort:       "day",
		startIndex: startIdx,
		maxResults: 10000,
	})
	if err != nil {
		return platform.FetchResult{}, fmt.Errorf("basic metrics: %w", err)
	}

	result := transformContentOwnerResponse(basicResp)

	// Query 2: Revenue metrics by day × country
	revenueResp, err := c.queryAnalytics(ctx, queryParams{
		startDate:  since,
		endDate:    time.Now().UTC(),
		metrics:    "estimatedPartnerRevenue",
		dimensions: "day,country",
		sort:       "day",
		maxResults: 10000,
	})
	if err != nil {
		fmt.Printf("[youtube] revenue query failed (non-fatal): %v\n", err)
	} else {
		revenueMetrics := transformRevenueResponse(revenueResp)
		result.Metrics = append(result.Metrics, revenueMetrics...)
	}

	return result, nil
}

// FetchEngagement retrieves traffic source breakdown data.
func (c *Client) FetchEngagement(ctx context.Context, since time.Time, cursor string) (platform.EngagementResult, error) {
	if err := c.quota.Use(ctx, 5); err != nil {
		return platform.EngagementResult{}, fmt.Errorf("youtube quota exceeded: %w", err)
	}

	resp, err := c.queryAnalytics(ctx, queryParams{
		startDate:  since,
		endDate:    time.Now().UTC(),
		metrics:    "views,estimatedMinutesWatched",
		dimensions: "day,insightTrafficSourceType",
		sort:       "day",
		maxResults: 10000,
	})
	if err != nil {
		return platform.EngagementResult{}, err
	}

	return transformTrafficSources(resp), nil
}

// FetchDemographics retrieves age/gender breakdown.
func (c *Client) FetchDemographics(ctx context.Context, since time.Time, cursor string) (platform.DemographicsResult, error) {
	if err := c.quota.Use(ctx, 5); err != nil {
		return platform.DemographicsResult{}, fmt.Errorf("youtube quota exceeded: %w", err)
	}

	resp, err := c.queryAnalytics(ctx, queryParams{
		startDate:  since,
		endDate:    time.Now().UTC(),
		metrics:    "viewerPercentage",
		dimensions: "ageGroup,gender",
		sort:       "-viewerPercentage",
		maxResults: 10000,
	})
	if err != nil {
		return platform.DemographicsResult{}, err
	}

	return transformDemographics(resp, since), nil
}

// queryParams holds common parameters for YouTube Analytics queries.
type queryParams struct {
	startDate  time.Time
	endDate    time.Time
	metrics    string
	dimensions string
	sort       string
	startIndex int
	maxResults int
}

// queryAnalytics executes a YouTube Analytics API query.
func (c *Client) queryAnalytics(ctx context.Context, p queryParams) (youtubeAnalyticsResponse, error) {
	u, err := url.Parse(analyticsURL)
	if err != nil {
		return youtubeAnalyticsResponse{}, fmt.Errorf("parse URL: %w", err)
	}

	q := u.Query()
	q.Set("ids", "contentOwner=="+c.contentOwnerID)
	q.Set("startDate", p.startDate.Format("2006-01-02"))
	q.Set("endDate", p.endDate.Format("2006-01-02"))
	q.Set("metrics", p.metrics)
	q.Set("dimensions", p.dimensions)
	if p.sort != "" {
		q.Set("sort", p.sort)
	}
	if p.startIndex > 0 {
		q.Set("startIndex", strconv.Itoa(p.startIndex))
	}
	q.Set("maxResults", strconv.Itoa(p.maxResults))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return youtubeAnalyticsResponse{}, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return youtubeAnalyticsResponse{}, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return youtubeAnalyticsResponse{}, fmt.Errorf("youtube API error %d: %s", resp.StatusCode, string(body))
	}

	var apiResp youtubeAnalyticsResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return youtubeAnalyticsResponse{}, fmt.Errorf("decode response: %w", err)
	}

	return apiResp, nil
}

// youtubeAnalyticsResponse is the YouTube Analytics API response shape.
type youtubeAnalyticsResponse struct {
	ColumnHeaders []struct {
		Name string `json:"name"`
	} `json:"columnHeaders"`
	Rows [][]json.RawMessage `json:"rows"`
}
