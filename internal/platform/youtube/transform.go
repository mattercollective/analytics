package youtube

import (
	"encoding/json"
	"time"

	"github.com/mattercollective/analytics-engine/internal/platform"
)

func transformResponse(resp youtubeAnalyticsResponse) platform.FetchResult {
	var metrics []platform.RawMetric

	// Build column index map
	colIdx := make(map[string]int)
	for i, col := range resp.ColumnHeaders {
		colIdx[col.Name] = i
	}

	for _, row := range resp.Rows {
		var videoID, dateStr, country string
		var views, watchMinutes, likes, shares, comments int64

		if idx, ok := colIdx["video"]; ok && idx < len(row) {
			json.Unmarshal(row[idx], &videoID)
		}
		if idx, ok := colIdx["day"]; ok && idx < len(row) {
			json.Unmarshal(row[idx], &dateStr)
		}
		if idx, ok := colIdx["country"]; ok && idx < len(row) {
			json.Unmarshal(row[idx], &country)
		}
		if idx, ok := colIdx["views"]; ok && idx < len(row) {
			json.Unmarshal(row[idx], &views)
		}
		if idx, ok := colIdx["estimatedMinutesWatched"]; ok && idx < len(row) {
			json.Unmarshal(row[idx], &watchMinutes)
		}
		if idx, ok := colIdx["likes"]; ok && idx < len(row) {
			json.Unmarshal(row[idx], &likes)
		}
		if idx, ok := colIdx["shares"]; ok && idx < len(row) {
			json.Unmarshal(row[idx], &shares)
		}
		if idx, ok := colIdx["comments"]; ok && idx < len(row) {
			json.Unmarshal(row[idx], &comments)
		}

		date, _ := time.Parse("2006-01-02", dateStr)

		base := platform.RawMetric{
			YTAssetID: videoID, // Will map to yt_asset_id or yt_channel_id
			Territory: country,
			Date:      date,
			RawData:   map[string]any{"source": "youtube_analytics"},
		}

		if views > 0 {
			m := base
			m.MetricType = "views"
			m.Value = views
			metrics = append(metrics, m)
		}

		if watchMinutes > 0 {
			m := base
			m.MetricType = "watch_time_hours"
			hours := float64(watchMinutes) / 60.0
			m.ValueDecimal = &hours
			m.Value = int64(hours)
			metrics = append(metrics, m)
		}

		if likes > 0 {
			m := base
			m.MetricType = "likes"
			m.Value = likes
			metrics = append(metrics, m)
		}

		if shares > 0 {
			m := base
			m.MetricType = "shares"
			m.Value = shares
			metrics = append(metrics, m)
		}

		if comments > 0 {
			m := base
			m.MetricType = "comments"
			m.Value = comments
			metrics = append(metrics, m)
		}
	}

	// YouTube Analytics paginates by row count
	hasMore := len(resp.Rows) >= 10000

	return platform.FetchResult{
		Metrics:    metrics,
		HasMore:    hasMore,
	}
}
