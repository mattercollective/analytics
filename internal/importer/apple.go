package importer

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/mattercollective/analytics-engine/internal/model"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

// AppleImporter loads Apple Music streams reports from GCS.
// Apple reports use an Apple Identifier (numeric ID) that must be mapped
// to ISRC via the amcontent file before metrics can be stored.
type AppleImporter struct {
	gcs         *GCSClient
	metricsRepo *repository.MetricsRepo
	logger      zerolog.Logger
	bucket      string

	// appleIDToISRC maps Apple Identifier -> ISRC
	appleIDToISRC map[string]string
}

func NewAppleImporter(gcs *GCSClient, metricsRepo *repository.MetricsRepo, logger zerolog.Logger, bucket string) *AppleImporter {
	return &AppleImporter{
		gcs:           gcs,
		metricsRepo:   metricsRepo,
		logger:        logger.With().Str("importer", "apple").Logger(),
		bucket:        bucket,
		appleIDToISRC: make(map[string]string),
	}
}

// LoadContentMapping reads the Apple Music content file to build the
// Apple Identifier -> ISRC lookup table.
func (a *AppleImporter) LoadContentMapping(ctx context.Context, contentPath string) error {
	a.logger.Info().Str("path", contentPath).Msg("loading Apple content mapping")

	reader, err := a.gcs.ReadFile(ctx, a.bucket, contentPath)
	if err != nil {
		return fmt.Errorf("read content file: %w", err)
	}
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	// Read header
	if !scanner.Scan() {
		return fmt.Errorf("empty content file")
	}
	header := scanner.Text()
	colIdx := parseTSVHeader(header)

	appleIDIdx, ok1 := colIdx["apple identifier"]
	isrcIdx, ok2 := colIdx["isrc"]
	if !ok1 || !ok2 {
		return fmt.Errorf("content file missing required columns (Apple Identifier, ISRC), got: %v", header)
	}

	count := 0
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if appleIDIdx >= len(fields) || isrcIdx >= len(fields) {
			continue
		}

		appleID := strings.TrimSpace(fields[appleIDIdx])
		isrc := strings.TrimSpace(fields[isrcIdx])
		if appleID != "" && isrc != "" {
			a.appleIDToISRC[appleID] = isrc
			count++
		}
	}

	a.logger.Info().Int("mappings", count).Msg("Apple content mapping loaded")
	return scanner.Err()
}

// ImportStreamsFile parses a single Apple Music SummaryStreams TSV file
// and upserts the aggregated stream counts into analytics.metrics.
//
// Apple streams files have one row per (date, apple_id, storefront, subscription_type, ...)
// with Streams=1 per row. We aggregate by (date, apple_id, storefront) to get daily totals.
func (a *AppleImporter) ImportStreamsFile(ctx context.Context, filePath string) (int, int, error) {
	reader, err := a.gcs.ReadFile(ctx, a.bucket, filePath)
	if err != nil {
		return 0, 0, fmt.Errorf("read streams file: %w", err)
	}
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	if !scanner.Scan() {
		return 0, 0, fmt.Errorf("empty streams file")
	}
	colIdx := parseTSVHeader(scanner.Text())

	dateIdx, ok1 := colIdx["datestamp"]
	appleIDIdx, ok2 := colIdx["apple identifier"]
	storefrontIdx, ok3 := colIdx["storefront name"]
	streamsIdx, ok4 := colIdx["streams"]
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return 0, 0, fmt.Errorf("streams file missing required columns")
	}

	// Aggregate: (date, isrc, territory) -> total streams
	type aggKey struct {
		date      string
		isrc      string
		territory string
	}
	agg := make(map[aggKey]int64)

	rowCount := 0
	unmapped := 0
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		rowCount++

		dateStr := getField(fields, dateIdx)
		appleID := getField(fields, appleIDIdx)
		territory := getField(fields, storefrontIdx)
		streamsStr := getField(fields, streamsIdx)

		isrc, ok := a.appleIDToISRC[appleID]
		if !ok {
			unmapped++
			continue
		}

		streams, _ := strconv.ParseInt(streamsStr, 10, 64)
		if streams <= 0 {
			streams = 1
		}

		key := aggKey{date: dateStr, isrc: isrc, territory: territory}
		agg[key] += streams
	}

	if err := scanner.Err(); err != nil {
		return 0, 0, fmt.Errorf("scan error: %w", err)
	}

	// Resolve ISRCs to asset_ids and build upserts
	var upserts []model.MetricUpsert
	for key, totalStreams := range agg {
		date, err := time.Parse("2006-01-02", key.date)
		if err != nil {
			continue
		}

		assetID, err := a.metricsRepo.ResolveAssetID(ctx, "isrc", key.isrc)
		if err != nil || assetID == nil {
			continue
		}

		territory := key.territory
		upserts = append(upserts, model.MetricUpsert{
			AssetID:    *assetID,
			PlatformID: "apple_music",
			Territory:  &territory,
			MetricDate: date,
			MetricType: model.MetricStreams,
			Value:      totalStreams,
		})
	}

	if len(upserts) == 0 {
		a.logger.Warn().
			Str("file", filePath).
			Int("rows", rowCount).
			Int("unmapped_apple_ids", unmapped).
			Msg("no metrics to upsert")
		return rowCount, 0, nil
	}

	inserted, updated, err := a.metricsRepo.BulkUpsert(ctx, upserts)
	if err != nil {
		return rowCount, 0, fmt.Errorf("bulk upsert: %w", err)
	}

	a.logger.Info().
		Str("file", filePath).
		Int("rows", rowCount).
		Int("aggregated", len(agg)).
		Int("unmapped", unmapped).
		Int("inserted", inserted).
		Int("updated", updated).
		Msg("Apple streams file imported")

	return rowCount, inserted + updated, nil
}

// ImportAllStreams imports all Apple Music daily streams files from the given prefix.
func (a *AppleImporter) ImportAllStreams(ctx context.Context, prefix string) error {
	files, err := a.gcs.ListFiles(ctx, a.bucket, prefix)
	if err != nil {
		return fmt.Errorf("list streams files: %w", err)
	}

	a.logger.Info().Int("files", len(files)).Str("prefix", prefix).Msg("starting Apple streams import")

	var totalRows, totalMetrics int
	for i, f := range files {
		rows, metrics, err := a.ImportStreamsFile(ctx, f)
		if err != nil {
			a.logger.Error().Err(err).Str("file", f).Msg("failed to import")
			continue
		}
		totalRows += rows
		totalMetrics += metrics

		if (i+1)%10 == 0 {
			a.logger.Info().Int("progress", i+1).Int("total", len(files)).Msg("import progress")
		}
	}

	a.logger.Info().
		Int("files", len(files)).
		Int("total_rows", totalRows).
		Int("total_metrics", totalMetrics).
		Msg("Apple streams import complete")

	return nil
}

// ReadZipCSV reads a CSV inside a zip archive from GCS.
func ReadZipCSV(data []byte) (io.Reader, error) {
	zipReader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}
	for _, f := range zipReader.File {
		if strings.HasSuffix(f.Name, ".csv") || strings.HasSuffix(f.Name, ".tsv") || strings.HasSuffix(f.Name, ".txt") {
			rc, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("open %s in zip: %w", f.Name, err)
			}
			return rc, nil
		}
	}
	return nil, fmt.Errorf("no CSV/TSV file found in zip archive")
}

func parseTSVHeader(line string) map[string]int {
	idx := make(map[string]int)
	for i, col := range strings.Split(line, "\t") {
		idx[strings.ToLower(strings.TrimSpace(col))] = i
	}
	return idx
}

func getField(fields []string, idx int) string {
	if idx < len(fields) {
		return strings.TrimSpace(fields[idx])
	}
	return ""
}
