package spotify

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/mattercollective/analytics-engine/internal/platform"
)

// transformAggregatedStreams converts Bulk API aggregatedstreams NDJSON records to RawMetrics.
// Each record contains: trackv2 (name, href, isrc), streams (total, country breakdowns),
// skips (total), saves (total).
func transformAggregatedStreams(records []map[string]any, date time.Time) []platform.RawMetric {
	var metrics []platform.RawMetric

	for _, rec := range records {
		isrc := extractISRC(rec)
		if isrc == "" {
			continue
		}

		base := platform.RawMetric{
			ISRC: isrc,
			Date: date,
			RawData: map[string]any{
				"source": "spotify_bulk",
			},
		}

		// Total streams
		if streams := extractNestedInt(rec, "streams", "total"); streams > 0 {
			m := base
			m.MetricType = "streams"
			m.Value = streams
			metrics = append(metrics, m)
		}

		// Total skips
		if skips := extractNestedInt(rec, "skips", "total"); skips > 0 {
			m := base
			m.MetricType = "saves" // skips stored separately in engagement table
			m.Value = skips
			// Note: we store skips in engagement table, not metrics; skip here
		}

		// Total saves
		if saves := extractNestedInt(rec, "saves", "total"); saves > 0 {
			m := base
			m.MetricType = "saves"
			m.Value = saves
			metrics = append(metrics, m)
		}

		// Listeners (unique user count if available)
		if listeners := extractInt(rec, "listeners"); listeners > 0 {
			m := base
			m.MetricType = "listeners"
			m.Value = listeners
			metrics = append(metrics, m)
		}

		// Country-level stream breakdowns
		if streamsObj, ok := rec["streams"].(map[string]any); ok {
			for key, val := range streamsObj {
				if key == "total" {
					continue
				}
				// Country-level data: key is country code
				if len(key) == 2 {
					countryStreams := toInt64(val)
					if countryStreams > 0 {
						m := base
						m.MetricType = "streams"
						m.Territory = key
						m.Value = countryStreams
						metrics = append(metrics, m)
					}
				}
			}
		}
	}

	return metrics
}

// aggregateStreamEngagement processes raw stream records into per-source engagement.
// Raw stream records have: track_id, source, source_uri, discovery_flag, completion_flag.
func aggregateStreamEngagement(records []map[string]any, date time.Time) []platform.RawEngagement {
	// Aggregate by (ISRC, territory, source)
	type key struct {
		ISRC      string
		Territory string
		Source    string
	}
	agg := make(map[key]*platform.RawEngagement)

	for _, rec := range records {
		// Raw streams use track_id not ISRC directly — need to join with tracks resource
		// For now, use ISRC if available in the record
		isrc := getString(rec, "isrc")
		if isrc == "" {
			continue
		}

		territory := getString(rec, "country")
		source := mapSource(getString(rec, "source"))
		sourceURI := getString(rec, "source_uri")

		k := key{ISRC: isrc, Territory: territory, Source: source}
		eng, ok := agg[k]
		if !ok {
			eng = &platform.RawEngagement{
				ISRC:      isrc,
				Territory: territory,
				Date:      date,
				Source:    source,
				SourceURI: sourceURI,
			}
			agg[k] = eng
		}

		eng.Streams++

		if getBool(rec, "discovery_flag") {
			eng.Discovery++
		}
		if getString(rec, "completion_flag") == "Y" {
			eng.Completions++
		}
	}

	results := make([]platform.RawEngagement, 0, len(agg))
	for _, eng := range agg {
		results = append(results, *eng)
	}
	return results
}

// extractDemographics pulls age/gender breakdowns from aggregated streams records.
// The aggregated streams resource nests: streams.total -> country -> sex -> age breakdowns.
func extractDemographics(records []map[string]any, date time.Time) []platform.RawDemographic {
	var demos []platform.RawDemographic

	for _, rec := range records {
		isrc := extractISRC(rec)
		if isrc == "" {
			continue
		}

		streamsObj, ok := rec["streams"].(map[string]any)
		if !ok {
			continue
		}

		// Iterate country -> sex -> age structure
		for country, countryData := range streamsObj {
			if country == "total" || len(country) != 2 {
				continue
			}

			countryMap, ok := countryData.(map[string]any)
			if !ok {
				continue
			}

			for gender, genderData := range countryMap {
				if gender == "total" {
					continue
				}
				mappedGender := mapGender(gender)

				genderMap, ok := genderData.(map[string]any)
				if !ok {
					// Flat value = total for this gender, no age breakdown
					demos = append(demos, platform.RawDemographic{
						ISRC:      isrc,
						Territory: country,
						Date:      date,
						AgeBucket: "unknown",
						Gender:    mappedGender,
						Streams:   toInt64(genderData),
					})
					continue
				}

				for ageBucket, ageVal := range genderMap {
					if ageBucket == "total" {
						continue
					}
					streams := toInt64(ageVal)
					if streams > 0 {
						demos = append(demos, platform.RawDemographic{
							ISRC:      isrc,
							Territory: country,
							Date:      date,
							AgeBucket: mapAgeBucket(ageBucket),
							Gender:    mappedGender,
							Streams:   streams,
						})
					}
				}
			}
		}
	}

	return demos
}

// -- Helpers --

func extractISRC(rec map[string]any) string {
	// aggregatedstreams: trackv2.isrc
	if trackv2, ok := rec["trackv2"].(map[string]any); ok {
		if isrc, ok := trackv2["isrc"].(string); ok {
			return isrc
		}
	}
	// Direct field
	if isrc, ok := rec["isrc"].(string); ok {
		return isrc
	}
	return ""
}

func extractNestedInt(rec map[string]any, keys ...string) int64 {
	var current any = rec
	for _, k := range keys {
		m, ok := current.(map[string]any)
		if !ok {
			return 0
		}
		current = m[k]
	}
	return toInt64(current)
}

func extractInt(rec map[string]any, key string) int64 {
	return toInt64(rec[key])
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}

func getString(rec map[string]any, key string) string {
	if v, ok := rec[key].(string); ok {
		return v
	}
	return ""
}

func getBool(rec map[string]any, key string) bool {
	if v, ok := rec[key].(bool); ok {
		return v
	}
	if v, ok := rec[key].(string); ok {
		return v == "true" || v == "Y"
	}
	return false
}

// mapSource normalizes Spotify source values to our standard source names.
func mapSource(source string) string {
	switch source {
	case "others_playlist", "playlist":
		return "playlist"
	case "radio":
		return "radio"
	case "search":
		return "search"
	case "album":
		return "album"
	case "artist":
		return "artist_page"
	case "collection":
		return "library"
	case "play_queue":
		return "queue"
	default:
		return "other"
	}
}

// mapGender normalizes Spotify gender values.
func mapGender(g string) string {
	switch g {
	case "male", "MALE":
		return "male"
	case "female", "FEMALE":
		return "female"
	case "neutral", "NEUTRAL", "non_binary":
		return "non_binary"
	default:
		return "unknown"
	}
}

// mapAgeBucket normalizes Spotify age group values.
func mapAgeBucket(ab string) string {
	switch ab {
	case "0-17":
		return "13-17"
	case "18-22":
		return "18-24"
	case "23-27":
		return "25-34"
	case "28-34":
		return "25-34"
	case "35-44":
		return "35-44"
	case "45-59":
		return "45-54"
	case "60-150":
		return "55-64"
	default:
		return ab
	}
}

// Ensure json is imported for json.Number
var _ = fmt.Sprintf
