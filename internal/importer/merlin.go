package importer

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/mattercollective/analytics-engine/internal/model"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

// MerlinImporter loads Merlin trends/usage reports from GCS.
// Merlin trends files are zipped CSVs with columns:
// week_start_date, week_end_date, merlin_licensor, country, rank, isrc,
// track_name, artists, age_bucket, gender, streams30s
type MerlinImporter struct {
	gcs         *GCSClient
	metricsRepo *repository.MetricsRepo
	logger      zerolog.Logger
	bucket      string
}

func NewMerlinImporter(gcs *GCSClient, metricsRepo *repository.MetricsRepo, logger zerolog.Logger, bucket string) *MerlinImporter {
	return &MerlinImporter{
		gcs:         gcs,
		metricsRepo: metricsRepo,
		logger:      logger.With().Str("importer", "merlin").Logger(),
		bucket:      bucket,
	}
}

// ImportTrendsFile parses a single Merlin trends CSV (possibly inside a zip) and upserts.
// Merlin trends break down by age_bucket and gender — we aggregate to (isrc, country, week_start_date).
func (m *MerlinImporter) ImportTrendsFile(ctx context.Context, filePath string) (int, int, error) {
	// Read the whole file — needed for zip extraction
	rawReader, err := m.gcs.ReadFile(ctx, m.bucket, filePath)
	if err != nil {
		return 0, 0, fmt.Errorf("read file: %w", err)
	}

	data, err := io.ReadAll(rawReader)
	rawReader.Close()
	if err != nil {
		return 0, 0, fmt.Errorf("read data: %w", err)
	}

	var csvReader *csv.Reader

	if strings.HasSuffix(filePath, ".csv.zip") || strings.HasSuffix(filePath, ".zip") {
		zipReader, err := ReadZipCSV(data)
		if err != nil {
			return 0, 0, fmt.Errorf("extract zip: %w", err)
		}
		csvReader = csv.NewReader(zipReader)
	} else {
		csvReader = csv.NewReader(bytes.NewReader(data))
	}

	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true

	header, err := csvReader.Read()
	if err != nil {
		return 0, 0, fmt.Errorf("read header: %w", err)
	}
	colIdx := parseCSVHeader(header)

	weekStartIdx, ok1 := colIdx["week_start_date"]
	countryIdx, ok2 := colIdx["country"]
	isrcIdx, ok3 := colIdx["isrc"]
	streamsIdx, ok4 := colIdx["streams30s"]

	if !ok1 || !ok2 || !ok3 || !ok4 {
		return 0, 0, fmt.Errorf("missing required columns (week_start_date, country, isrc, streams30s)")
	}

	// Determine sub-platform from file path (e.g., "spo-spotify" -> "spotify")
	platformID := extractMerlinPlatform(filePath)

	// Aggregate: (isrc, country, week_start_date) -> total streams
	type aggKey struct {
		isrc    string
		country string
		date    string
	}
	agg := make(map[aggKey]int64)

	rowCount := 0
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		rowCount++

		isrc := getCSVField(row, isrcIdx)
		country := getCSVField(row, countryIdx)
		weekStart := getCSVField(row, weekStartIdx)
		streams, _ := strconv.ParseInt(getCSVField(row, streamsIdx), 10, 64)

		if isrc == "" || streams <= 0 {
			continue
		}

		key := aggKey{isrc: isrc, country: country, date: weekStart}
		agg[key] += streams
	}

	// Build upserts
	var upserts []model.MetricUpsert
	for key, total := range agg {
		date, err := time.Parse("2006-01-02", key.date)
		if err != nil {
			continue
		}

		assetID, err := m.metricsRepo.ResolveAssetID(ctx, "isrc", key.isrc)
		if err != nil || assetID == nil {
			continue
		}

		country := key.country
		upserts = append(upserts, model.MetricUpsert{
			AssetID:    *assetID,
			PlatformID: platformID,
			Territory:  &country,
			MetricDate: date,
			MetricType: model.MetricStreams,
			Value:      total,
		})
	}

	if len(upserts) == 0 {
		m.logger.Warn().Str("file", filePath).Int("rows", rowCount).Msg("no metrics to upsert")
		return rowCount, 0, nil
	}

	inserted, updated, err := m.metricsRepo.BulkUpsert(ctx, upserts)
	if err != nil {
		return rowCount, 0, fmt.Errorf("bulk upsert: %w", err)
	}

	m.logger.Info().
		Str("file", filePath).
		Str("platform", platformID).
		Int("rows", rowCount).
		Int("aggregated", len(agg)).
		Int("inserted", inserted).
		Int("updated", updated).
		Msg("Merlin trends file imported")

	return rowCount, inserted + updated, nil
}

// ImportAllTrends imports all Merlin trends files under the given prefix.
func (m *MerlinImporter) ImportAllTrends(ctx context.Context, prefix string) error {
	files, err := m.gcs.ListFiles(ctx, m.bucket, prefix)
	if err != nil {
		return fmt.Errorf("list files: %w", err)
	}

	// Filter to only .zip and .csv files
	var importFiles []string
	for _, f := range files {
		if strings.HasSuffix(f, ".csv.zip") || strings.HasSuffix(f, ".csv") || strings.HasSuffix(f, ".zip") {
			importFiles = append(importFiles, f)
		}
	}

	m.logger.Info().Int("files", len(importFiles)).Str("prefix", prefix).Msg("starting Merlin trends import")

	var totalRows, totalMetrics int
	for i, f := range importFiles {
		rows, metrics, err := m.ImportTrendsFile(ctx, f)
		if err != nil {
			m.logger.Error().Err(err).Str("file", f).Msg("failed to import")
			continue
		}
		totalRows += rows
		totalMetrics += metrics

		if (i+1)%5 == 0 {
			m.logger.Info().Int("progress", i+1).Int("total", len(importFiles)).Msg("import progress")
		}
	}

	m.logger.Info().
		Int("files", len(importFiles)).
		Int("total_rows", totalRows).
		Int("total_metrics", totalMetrics).
		Msg("Merlin trends import complete")

	return nil
}

// extractMerlinPlatform maps a Merlin file path to a platform_id.
// Paths look like: merlin/trends/202603/spo-spotify/weekly-topd/file.csv.zip
func extractMerlinPlatform(path string) string {
	platformMap := map[string]string{
		"spo-spotify":            "spotify",
		"dzr-deezer":             "deezer",
		"fbk-facebook":           "facebook",
		"aum-audiomack":          "audiomack",
		"awa-awa":                "awa",
		"boo-boomplay":           "boomplay",
		"ncm-netease-cloud-music": "netease",
		"plt-peloton":            "peloton",
		"tme-tencent":            "tencent",
		"vvo-vevo":               "vevo",
	}

	parts := strings.Split(path, "/")
	for _, part := range parts {
		if pid, ok := platformMap[part]; ok {
			return pid
		}
	}

	return "merlin" // fallback
}
