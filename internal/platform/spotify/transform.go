package spotify

import (
	"time"

	"github.com/mattercollective/analytics-engine/internal/platform"
)

// transformResponse converts a Spotify bulk API response into generic RawMetrics.
func transformResponse(resp bulkAPIResponse) platform.FetchResult {
	var metrics []platform.RawMetric

	for _, rec := range resp.Data {
		date, _ := time.Parse("2006-01-02", rec.Date)
		territory := rec.Territory

		base := platform.RawMetric{
			ISRC:      rec.ISRC,
			Territory: territory,
			Date:      date,
			RawData:   map[string]any{"source": "spotify_bulk"},
		}

		if rec.Streams > 0 {
			m := base
			m.MetricType = "streams"
			m.Value = rec.Streams
			metrics = append(metrics, m)
		}

		if rec.Listeners > 0 {
			m := base
			m.MetricType = "listeners"
			m.Value = rec.Listeners
			metrics = append(metrics, m)
		}

		if rec.Saves > 0 {
			m := base
			m.MetricType = "saves"
			m.Value = rec.Saves
			metrics = append(metrics, m)
		}

		if rec.PlaylistAdds > 0 {
			m := base
			m.MetricType = "playlist_adds"
			m.Value = rec.PlaylistAdds
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
