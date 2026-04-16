package apple

import (
	"time"

	"github.com/mattercollective/analytics-engine/internal/platform"
)

// transformEngagementResponse converts Apple Music audience-engagement results to RawMetrics.
func transformEngagementResponse(resp audienceEngagementResponse) platform.FetchResult {
	var metrics []platform.RawMetric

	for _, rec := range resp.Results {
		date, _ := time.Parse("2006-01-02", rec.Date)

		// Use ISRC if available, fall back to song_id
		isrc := rec.ISRC
		if isrc == "" {
			isrc = rec.SongID
		}

		base := platform.RawMetric{
			ISRC:      isrc,
			Territory: rec.Storefront,
			Date:      date,
			RawData:   map[string]any{"source": "apple_analytics"},
		}

		if rec.Plays > 0 {
			m := base
			m.MetricType = "streams"
			m.Value = rec.Plays
			metrics = append(metrics, m)
		}

		if rec.Listeners > 0 {
			m := base
			m.MetricType = "listeners"
			m.Value = rec.Listeners
			metrics = append(metrics, m)
		}
	}

	var nextCursor string
	hasMore := false
	if resp.Next != nil && *resp.Next != "" {
		nextCursor = *resp.Next
		hasMore = true
	}

	return platform.FetchResult{
		Metrics:    metrics,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}
}
