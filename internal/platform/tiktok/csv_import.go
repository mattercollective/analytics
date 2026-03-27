package tiktok

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/mattercollective/analytics-engine/internal/platform"
)

// ParseCSV reads a TikTok analytics CSV export and returns RawMetrics.
// Expected columns: isrc, date, territory, sound_uses, views, likes, shares
func ParseCSV(r io.Reader) ([]platform.RawMetric, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read CSV header: %w", err)
	}

	colIdx := make(map[string]int)
	for i, col := range header {
		colIdx[strings.ToLower(strings.TrimSpace(col))] = i
	}

	requiredCols := []string{"isrc", "date"}
	for _, col := range requiredCols {
		if _, ok := colIdx[col]; !ok {
			return nil, fmt.Errorf("missing required column: %s", col)
		}
	}

	var metrics []platform.RawMetric

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read CSV row: %w", err)
		}

		isrc := getCol(row, colIdx, "isrc")
		dateStr := getCol(row, colIdx, "date")
		territory := getCol(row, colIdx, "territory")

		date, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			continue // skip unparseable rows
		}

		base := platform.RawMetric{
			ISRC:      isrc,
			Territory: territory,
			Date:      date,
			RawData:   map[string]any{"source": "tiktok_csv"},
		}

		metricCols := map[string]string{
			"sound_uses": "sound_uses",
			"views":      "views",
			"likes":      "likes",
			"shares":     "shares",
		}

		for csvCol, metricType := range metricCols {
			if val := getColInt(row, colIdx, csvCol); val > 0 {
				m := base
				m.MetricType = metricType
				m.Value = val
				metrics = append(metrics, m)
			}
		}
	}

	return metrics, nil
}

func getCol(row []string, colIdx map[string]int, name string) string {
	if idx, ok := colIdx[name]; ok && idx < len(row) {
		return strings.TrimSpace(row[idx])
	}
	return ""
}

func getColInt(row []string, colIdx map[string]int, name string) int64 {
	s := getCol(row, colIdx, name)
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}
