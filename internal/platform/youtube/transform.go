package youtube

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/mattercollective/analytics-engine/internal/platform"
)

func transformResponse(resp youtubeAnalyticsResponse) platform.FetchResult {
	var metrics []platform.RawMetric

	colIdx := buildColIndex(resp)

	for _, row := range resp.Rows {
		videoID := getString(row, colIdx, "video")
		dateStr := getString(row, colIdx, "day")
		country := getString(row, colIdx, "country")

		date, _ := time.Parse("2006-01-02", dateStr)

		base := platform.RawMetric{
			YTAssetID: videoID,
			Territory: country,
			Date:      date,
			RawData:   map[string]any{"source": "youtube_analytics"},
		}

		// Views
		if v := getNumber(row, colIdx, "views"); v > 0 {
			m := base
			m.MetricType = "views"
			m.Value = v
			metrics = append(metrics, m)
		}

		// Watch time
		if mins := getNumber(row, colIdx, "estimatedMinutesWatched"); mins > 0 {
			m := base
			m.MetricType = "watch_time_hours"
			hours := float64(mins) / 60.0
			m.ValueDecimal = &hours
			m.Value = int64(hours)
			metrics = append(metrics, m)
		}

		// Likes
		if v := getNumber(row, colIdx, "likes"); v > 0 {
			m := base
			m.MetricType = "likes"
			m.Value = v
			metrics = append(metrics, m)
		}

		// Shares
		if v := getNumber(row, colIdx, "shares"); v > 0 {
			m := base
			m.MetricType = "shares"
			m.Value = v
			metrics = append(metrics, m)
		}

		// Comments
		if v := getNumber(row, colIdx, "comments"); v > 0 {
			m := base
			m.MetricType = "comments"
			m.Value = v
			metrics = append(metrics, m)
		}

		// Revenue — total partner revenue
		if rev := getFloat(row, colIdx, "estimatedPartnerRevenue"); rev > 0 {
			m := base
			m.MetricType = "revenue_estimate"
			m.ValueDecimal = &rev
			m.Value = int64(rev * 100) // cents
			metrics = append(metrics, m)
		}
	}

	hasMore := len(resp.Rows) >= 10000

	return platform.FetchResult{
		Metrics: metrics,
		HasMore: hasMore,
	}
}

// transformContentOwnerResponse converts content owner level analytics (day × country) to RawMetrics.
// Content owner queries don't include video dimension — metrics are aggregate across all content.
func transformContentOwnerResponse(resp youtubeAnalyticsResponse) platform.FetchResult {
	var metrics []platform.RawMetric

	colIdx := buildColIndex(resp)

	for _, row := range resp.Rows {
		dateStr := getString(row, colIdx, "day")
		country := getString(row, colIdx, "country")
		date, _ := time.Parse("2006-01-02", dateStr)

		base := platform.RawMetric{
			Territory: country,
			Date:      date,
			RawData:   map[string]any{"source": "youtube_analytics"},
		}

		if v := getNumber(row, colIdx, "views"); v > 0 {
			m := base
			m.MetricType = "views"
			m.Value = v
			metrics = append(metrics, m)
		}

		if mins := getNumber(row, colIdx, "estimatedMinutesWatched"); mins > 0 {
			m := base
			m.MetricType = "watch_time_hours"
			hours := float64(mins) / 60.0
			m.ValueDecimal = &hours
			m.Value = int64(hours)
			metrics = append(metrics, m)
		}

		if v := getNumber(row, colIdx, "likes"); v > 0 {
			m := base
			m.MetricType = "likes"
			m.Value = v
			metrics = append(metrics, m)
		}

		if v := getNumber(row, colIdx, "shares"); v > 0 {
			m := base
			m.MetricType = "shares"
			m.Value = v
			metrics = append(metrics, m)
		}

		if v := getNumber(row, colIdx, "comments"); v > 0 {
			m := base
			m.MetricType = "comments"
			m.Value = v
			metrics = append(metrics, m)
		}
	}

	return platform.FetchResult{
		Metrics: metrics,
		HasMore: len(resp.Rows) >= 10000,
	}
}

// transformRevenueResponse converts YouTube Analytics revenue data (day × country) to RawMetrics.
func transformRevenueResponse(resp youtubeAnalyticsResponse) []platform.RawMetric {
	var metrics []platform.RawMetric

	colIdx := buildColIndex(resp)

	for _, row := range resp.Rows {
		dateStr := getString(row, colIdx, "day")
		country := getString(row, colIdx, "country")
		date, _ := time.Parse("2006-01-02", dateStr)

		base := platform.RawMetric{
			Territory: country,
			Date:      date,
			RawData:   map[string]any{"source": "youtube_analytics"},
		}

		if rev := getFloat(row, colIdx, "estimatedPartnerRevenue"); rev > 0 {
			m := base
			m.MetricType = "revenue_estimate"
			m.ValueDecimal = &rev
			m.Value = int64(rev * 100)
			metrics = append(metrics, m)
		}
	}

	return metrics
}

// transformTrafficSources converts YouTube Analytics traffic source data to engagement records.
func transformTrafficSources(resp youtubeAnalyticsResponse) platform.EngagementResult {
	var records []platform.RawEngagement

	colIdx := buildColIndex(resp)

	for _, row := range resp.Rows {
		dateStr := getString(row, colIdx, "day")
		sourceType := getString(row, colIdx, "insightTrafficSourceType")

		date, _ := time.Parse("2006-01-02", dateStr)
		views := getNumber(row, colIdx, "views")
		watchMins := getNumber(row, colIdx, "estimatedMinutesWatched")

		if views == 0 {
			continue
		}

		records = append(records, platform.RawEngagement{
			Territory: "",
			Date:      date,
			Source:    mapTrafficSource(sourceType),
			Streams:   views,
			Completions: watchMins,
		})
	}

	return platform.EngagementResult{
		Records: records,
		HasMore: len(resp.Rows) >= 10000,
	}
}

// transformDemographics converts YouTube Analytics age/gender data to demographic records.
func transformDemographics(resp youtubeAnalyticsResponse, since time.Time) platform.DemographicsResult {
	var records []platform.RawDemographic

	colIdx := buildColIndex(resp)

	for _, row := range resp.Rows {
		videoID := getString(row, colIdx, "video")
		ageGroup := getString(row, colIdx, "ageGroup")
		gender := getString(row, colIdx, "gender")
		pct := getFloat(row, colIdx, "viewerPercentage")

		if pct == 0 {
			continue
		}

		records = append(records, platform.RawDemographic{
			ISRC:      "",
			Territory: "",
			Date:      since, // Demographics are aggregated over the date range
			AgeBucket: mapYTAgeBucket(ageGroup),
			Gender:    mapYTGender(gender),
			Streams:   int64(pct * 100), // Store as percentage * 100 for precision
			Listeners: 0,
		})

		_ = videoID // Will need to resolve to ISRC for per-asset demographics
	}

	return platform.DemographicsResult{
		Records: records,
		HasMore: len(resp.Rows) >= 10000,
	}
}

// -- Helpers --

func buildColIndex(resp youtubeAnalyticsResponse) map[string]int {
	idx := make(map[string]int)
	for i, col := range resp.ColumnHeaders {
		idx[col.Name] = i
	}
	return idx
}

func getString(row []json.RawMessage, colIdx map[string]int, key string) string {
	idx, ok := colIdx[key]
	if !ok || idx >= len(row) {
		return ""
	}
	var s string
	json.Unmarshal(row[idx], &s)
	return s
}

func getNumber(row []json.RawMessage, colIdx map[string]int, key string) int64 {
	idx, ok := colIdx[key]
	if !ok || idx >= len(row) {
		return 0
	}
	var n json.Number
	if err := json.Unmarshal(row[idx], &n); err != nil {
		return 0
	}
	i, err := n.Int64()
	if err != nil {
		f, _ := n.Float64()
		return int64(f)
	}
	return i
}

func getFloat(row []json.RawMessage, colIdx map[string]int, key string) float64 {
	idx, ok := colIdx[key]
	if !ok || idx >= len(row) {
		return 0
	}
	var s string
	if err := json.Unmarshal(row[idx], &s); err == nil {
		f, _ := strconv.ParseFloat(s, 64)
		return f
	}
	var f float64
	json.Unmarshal(row[idx], &f)
	return f
}

// mapTrafficSource normalizes YouTube traffic source types to our standard names.
func mapTrafficSource(src string) string {
	switch src {
	case "SUBSCRIBER":
		return "browse"
	case "EXT_URL":
		return "external"
	case "YT_SEARCH":
		return "search"
	case "RELATED_VIDEO":
		return "suggested"
	case "YT_CHANNEL":
		return "artist_page"
	case "YT_PLAYLIST_PAGE":
		return "playlist"
	case "NOTIFICATION":
		return "notification"
	case "SHORTS":
		return "shorts"
	case "NO_LINK_EMBEDDED":
		return "embedded"
	case "ADVERTISING":
		return "advertising"
	default:
		return "other"
	}
}

// mapYTAgeBucket normalizes YouTube age groups.
func mapYTAgeBucket(ag string) string {
	switch ag {
	case "age13-17":
		return "13-17"
	case "age18-24":
		return "18-24"
	case "age25-34":
		return "25-34"
	case "age35-44":
		return "35-44"
	case "age45-54":
		return "45-54"
	case "age55-64":
		return "55-64"
	case "age65-":
		return "65+"
	default:
		return ag
	}
}

// mapYTGender normalizes YouTube gender values.
func mapYTGender(g string) string {
	switch g {
	case "male", "MALE":
		return "male"
	case "female", "FEMALE":
		return "female"
	default:
		return "unknown"
	}
}
