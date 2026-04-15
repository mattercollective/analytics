package spotify

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/mattercollective/analytics-engine/internal/platform"
)

const (
	baseURL     = "https://api.spotify.com"
	bulkBaseURL = "https://provider-api.spotify.com/v1/analytics"
)

// Client implements platform.Fetcher and platform.EngagementFetcher
// for the Spotify Enhanced Bulk API.
type Client struct {
	httpClient  *http.Client
	limiter     *rate.Limiter
	licensorID  string
}

// NewClient creates a Spotify fetcher with rate limiting.
func NewClient(ctx context.Context, clientID, clientSecret, licensorID string) *Client {
	httpClient := NewHTTPClient(ctx, clientID, clientSecret)

	// Spotify rolling 30-second window rate limit
	limiter := rate.NewLimiter(rate.Every(time.Second), 10)
	httpClient.Transport = platform.NewRateLimitedTransport(httpClient.Transport, limiter)

	return &Client{
		httpClient:  httpClient,
		limiter:     limiter,
		licensorID:  licensorID,
	}
}

func (c *Client) Platform() string {
	return "spotify"
}

// FetchSince retrieves aggregated stream data from the Bulk API.
// Uses the aggregatedstreams resource which provides daily totals with demographic breakdowns.
func (c *Client) FetchSince(ctx context.Context, since time.Time, cursor string) (platform.FetchResult, error) {
	var allMetrics []platform.RawMetric

	// Iterate over each day in the range
	end := time.Now().UTC().Truncate(24 * time.Hour)
	for d := since; !d.After(end); d = d.AddDate(0, 0, 1) {
		url := fmt.Sprintf("%s/%s/enhanced/aggregatedstreams/%d/%02d/%02d",
			bulkBaseURL, c.licensorID, d.Year(), d.Month(), d.Day())

		metrics, err := c.fetchAggregatedStreams(ctx, url, d)
		if err != nil {
			fmt.Printf("[spotify] aggregatedstreams %s: %v\n", d.Format("2006-01-02"), err)
			continue
		}
		fmt.Printf("[spotify] aggregatedstreams %s: %d records\n", d.Format("2006-01-02"), len(metrics))
		allMetrics = append(allMetrics, metrics...)
	}

	return platform.FetchResult{
		Metrics:    allMetrics,
		NextCursor: "",
		HasMore:    false,
	}, nil
}

// fetchAggregatedStreams fetches a single day's aggregated stream data.
// Response is gzipped NDJSON.
func (c *Client) fetchAggregatedStreams(ctx context.Context, url string, date time.Time) ([]platform.RawMetric, error) {
	records, err := c.fetchNDJSON(ctx, url)
	if err != nil {
		return nil, err
	}

	return transformAggregatedStreams(records, date), nil
}

// FetchEngagement retrieves per-source engagement data from raw streams.
// Uses the streams resource which has source, source_uri, discovery_flag, etc.
func (c *Client) FetchEngagement(ctx context.Context, since time.Time, cursor string) (platform.EngagementResult, error) {
	var allRecords []platform.RawEngagement

	end := time.Now().UTC().Truncate(24 * time.Hour)
	for d := since; !d.After(end); d = d.AddDate(0, 0, 1) {
		// First get available countries for this day
		countriesURL := fmt.Sprintf("%s/%s/enhanced/streams/%d/%02d/%02d",
			bulkBaseURL, c.licensorID, d.Year(), d.Month(), d.Day())

		// Fetch the stream data (may be partitioned by country)
		records, err := c.fetchStreamEngagement(ctx, countriesURL, d)
		if err != nil {
			continue
		}
		allRecords = append(allRecords, records...)
	}

	return platform.EngagementResult{
		Records:    allRecords,
		NextCursor: "",
		HasMore:    false,
	}, nil
}

// fetchStreamEngagement processes raw stream records into source-level engagement aggregates.
func (c *Client) fetchStreamEngagement(ctx context.Context, url string, date time.Time) ([]platform.RawEngagement, error) {
	records, err := c.fetchNDJSON(ctx, url)
	if err != nil {
		return nil, err
	}

	return aggregateStreamEngagement(records, date), nil
}

// FetchDemographics retrieves user demographic data.
// Uses the users resource joined with streams data for ISRC mapping.
func (c *Client) FetchDemographics(ctx context.Context, since time.Time, cursor string) (platform.DemographicsResult, error) {
	var allRecords []platform.RawDemographic

	end := time.Now().UTC().Truncate(24 * time.Hour)
	for d := since; !d.After(end); d = d.AddDate(0, 0, 1) {
		url := fmt.Sprintf("%s/%s/enhanced/aggregatedstreams/%d/%02d/%02d",
			bulkBaseURL, c.licensorID, d.Year(), d.Month(), d.Day())

		records, err := c.fetchNDJSON(ctx, url)
		if err != nil {
			continue
		}

		allRecords = append(allRecords, extractDemographics(records, d)...)
	}

	return platform.DemographicsResult{
		Records:    allRecords,
		NextCursor: "",
		HasMore:    false,
	}, nil
}

// fetchNDJSON fetches a gzipped NDJSON endpoint and returns parsed records.
// The Bulk API returns a 303 redirect to a signed GCS URL for the .gz file.
func (c *Client) fetchNDJSON(ctx context.Context, url string) ([]map[string]any, error) {
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limit wait: %w", err)
	}

	// Step 1: Hit the Spotify endpoint (returns 303 redirect to signed GCS URL)
	// Don't follow the redirect — we need to use a plain client for GCS
	noRedirectClient := &http.Client{
		Transport: c.httpClient.Transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := noRedirectClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, fmt.Errorf("no data available")
	}

	// The API returns 303 with the GCS download URL in Location header or response body
	var downloadURL string
	if resp.StatusCode == http.StatusSeeOther || resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusTemporaryRedirect {
		downloadURL = resp.Header.Get("Location")
		if downloadURL == "" {
			body, _ := io.ReadAll(resp.Body)
			downloadURL = strings.TrimSpace(string(body))
		}
		resp.Body.Close()
	} else if resp.StatusCode == http.StatusOK {
		// May return the URL as the response body
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		downloadURL = strings.TrimSpace(string(body))
		if len(downloadURL) > 0 && downloadURL[0] == '[' {
			return nil, fmt.Errorf("directory listing, not a file")
		}
	} else {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("spotify API error %d: %s", resp.StatusCode, string(body))
	}

	if downloadURL == "" {
		return nil, fmt.Errorf("no download URL in response")
	}

	// Step 2: Download the gzipped file from GCS (no auth needed — URL is pre-signed)
	gcsReq, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create GCS request: %w", err)
	}

	gcsResp, err := http.DefaultClient.Do(gcsReq)
	if err != nil {
		return nil, fmt.Errorf("download from GCS: %w", err)
	}
	defer gcsResp.Body.Close()

	if gcsResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(gcsResp.Body)
		return nil, fmt.Errorf("GCS download error %d: %s", gcsResp.StatusCode, string(body))
	}

	// Step 3: Decompress gzip
	gzReader, err := gzip.NewReader(gcsResp.Body)
	if err != nil {
		return nil, fmt.Errorf("gzip decode: %w", err)
	}
	defer gzReader.Close()

	// Parse NDJSON (one JSON object per line)
	var records []map[string]any
	scanner := bufio.NewScanner(gzReader)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			continue
		}
		records = append(records, record)
	}

	return records, scanner.Err()
}
