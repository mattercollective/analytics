package importer

import (
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

// AmazonImporter loads Amazon Music summary statement CSVs from GCS.
// Amazon files have columns: VENDOR_CODE, TERRITORY_CODE, START_DATE, END_DATE,
// UPC, ISRC, PRODUCT_TYPE_ID, ..., UNITS, COST, AMOUNT, SALE_RETURN_FLAG, SALE_TYPE
type AmazonImporter struct {
	gcs         *GCSClient
	metricsRepo *repository.MetricsRepo
	logger      zerolog.Logger
	bucket      string
}

func NewAmazonImporter(gcs *GCSClient, metricsRepo *repository.MetricsRepo, logger zerolog.Logger, bucket string) *AmazonImporter {
	return &AmazonImporter{
		gcs:         gcs,
		metricsRepo: metricsRepo,
		logger:      logger.With().Str("importer", "amazon").Logger(),
		bucket:      bucket,
	}
}

// ImportFile parses a single Amazon summary statement CSV and upserts into analytics.metrics.
func (a *AmazonImporter) ImportFile(ctx context.Context, filePath string) (int, int, error) {
	reader, err := a.gcs.ReadFile(ctx, a.bucket, filePath)
	if err != nil {
		return 0, 0, fmt.Errorf("read file: %w", err)
	}
	defer reader.Close()

	csvReader := csv.NewReader(reader)
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true

	header, err := csvReader.Read()
	if err != nil {
		return 0, 0, fmt.Errorf("read header: %w", err)
	}
	colIdx := parseCSVHeader(header)

	isrcIdx, hasISRC := colIdx["isrc"]
	_, hasUPC := colIdx["upc"]
	territoryIdx, hasTerritory := colIdx["territory_code"]
	startDateIdx, hasStartDate := colIdx["start_date"]
	unitsIdx, hasUnits := colIdx["units"]
	productTypeIdx, hasProductType := colIdx["product_type_id"]
	amountIdx, hasAmount := colIdx["amount"]

	if !hasStartDate || !hasUnits {
		return 0, 0, fmt.Errorf("missing required columns (START_DATE, UNITS)")
	}

	// Aggregate: (isrc, territory, month_date, metric_type) -> total
	type aggKey struct {
		isrc       string
		territory  string
		date       string
		metricType model.MetricType
	}
	agg := make(map[aggKey]int64)
	revenueAgg := make(map[aggKey]float64)

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

		isrc := ""
		if hasISRC {
			isrc = getCSVField(row, isrcIdx)
		}
		if isrc == "" && hasUPC {
			// Album-level row, skip for now (no per-track ISRC)
			continue
		}

		territory := ""
		if hasTerritory {
			territory = getCSVField(row, territoryIdx)
		}

		dateStr := getCSVField(row, startDateIdx)
		// Amazon dates are YYYYMMDD
		date, err := time.Parse("20060102", dateStr)
		if err != nil {
			continue
		}
		monthStr := date.Format("2006-01-02")

		units, _ := strconv.ParseInt(getCSVField(row, unitsIdx), 10, 64)

		// Determine metric type from PRODUCT_TYPE_ID
		metricType := model.MetricDownloads
		if hasProductType {
			pt := getCSVField(row, productTypeIdx)
			if pt == "S" || pt == "ST" {
				metricType = model.MetricStreams
			}
		}
		// If units > 0, it's a sale; Amazon reports are primarily downloads/purchases
		if units > 0 {
			key := aggKey{isrc: isrc, territory: territory, date: monthStr, metricType: metricType}
			agg[key] += units
		}

		// Track revenue estimate
		if hasAmount {
			amount, _ := strconv.ParseFloat(getCSVField(row, amountIdx), 64)
			if amount > 0 {
				key := aggKey{isrc: isrc, territory: territory, date: monthStr, metricType: model.MetricRevenueEstimate}
				revenueAgg[key] += amount
			}
		}
	}

	// Build upserts
	var upserts []model.MetricUpsert

	for key, total := range agg {
		date, _ := time.Parse("2006-01-02", key.date)
		assetID, err := a.metricsRepo.ResolveAssetID(ctx, "isrc", key.isrc)
		if err != nil || assetID == nil {
			continue
		}

		territory := key.territory
		upserts = append(upserts, model.MetricUpsert{
			AssetID:    *assetID,
			PlatformID: "amazon_music",
			Territory:  &territory,
			MetricDate: date,
			MetricType: key.metricType,
			Value:      total,
		})
	}

	for key, amount := range revenueAgg {
		date, _ := time.Parse("2006-01-02", key.date)
		assetID, err := a.metricsRepo.ResolveAssetID(ctx, "isrc", key.isrc)
		if err != nil || assetID == nil {
			continue
		}

		territory := key.territory
		upserts = append(upserts, model.MetricUpsert{
			AssetID:      *assetID,
			PlatformID:   "amazon_music",
			Territory:    &territory,
			MetricDate:   date,
			MetricType:   model.MetricRevenueEstimate,
			Value:        int64(amount * 100), // cents
			ValueDecimal: &amount,
		})
	}

	if len(upserts) == 0 {
		a.logger.Warn().Str("file", filePath).Int("rows", rowCount).Msg("no metrics to upsert")
		return rowCount, 0, nil
	}

	inserted, updated, err := a.metricsRepo.BulkUpsert(ctx, upserts)
	if err != nil {
		return rowCount, 0, fmt.Errorf("bulk upsert: %w", err)
	}

	a.logger.Info().
		Str("file", filePath).
		Int("rows", rowCount).
		Int("inserted", inserted).
		Int("updated", updated).
		Msg("Amazon file imported")

	return rowCount, inserted + updated, nil
}

// ImportAll imports all Amazon CSV files under the given prefix.
func (a *AmazonImporter) ImportAll(ctx context.Context, prefix string) error {
	files, err := a.gcs.ListFiles(ctx, a.bucket, prefix)
	if err != nil {
		return fmt.Errorf("list files: %w", err)
	}

	a.logger.Info().Int("files", len(files)).Str("prefix", prefix).Msg("starting Amazon import")

	var totalRows, totalMetrics int
	for i, f := range files {
		if !strings.HasSuffix(f, ".csv") {
			continue
		}
		rows, metrics, err := a.ImportFile(ctx, f)
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
		Msg("Amazon import complete")

	return nil
}

func parseCSVHeader(header []string) map[string]int {
	idx := make(map[string]int)
	for i, col := range header {
		idx[strings.ToLower(strings.TrimSpace(col))] = i
	}
	return idx
}

func getCSVField(row []string, idx int) string {
	if idx < len(row) {
		return strings.TrimSpace(row[idx])
	}
	return ""
}
