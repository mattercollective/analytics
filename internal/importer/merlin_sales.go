package importer

import (
	"archive/zip"
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

// MerlinSalesImporter imports Merlin monthly sales reports across all platforms.
// Each platform has its own column naming but all share ISRC, territory, quantity, revenue.
type MerlinSalesImporter struct {
	gcs         *GCSClient
	metricsRepo *repository.MetricsRepo
	revenueRepo *repository.RevenueRepo
	logger      zerolog.Logger
	bucket      string
}

func NewMerlinSalesImporter(
	gcs *GCSClient,
	metricsRepo *repository.MetricsRepo,
	revenueRepo *repository.RevenueRepo,
	logger zerolog.Logger,
	bucket string,
) *MerlinSalesImporter {
	return &MerlinSalesImporter{
		gcs:         gcs,
		metricsRepo: metricsRepo,
		revenueRepo: revenueRepo,
		logger:      logger.With().Str("importer", "merlin_sales").Logger(),
		bucket:      bucket,
	}
}

// platformMapping maps Merlin folder codes to our platform IDs and column names.
type platformMapping struct {
	PlatformID   string
	DisplayName  string
	ISRCCol      []string // possible column names for ISRC
	CountryCol   []string // possible column names for territory
	QuantityCol  []string // possible column names for stream count
	RevenueCol   []string // possible column names for USD revenue
	DateStartCol []string // possible column names for start date
	DateEndCol   []string // possible column names for end date
}

var merlinPlatforms = map[string]platformMapping{
	"spo-spotify": {
		PlatformID: "spotify", DisplayName: "Spotify",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"country"},
		QuantityCol: []string{"quantity"}, RevenueCol: []string{"payable"},
		DateStartCol: []string{"start date"}, DateEndCol: []string{"end date"},
	},
	"dzr-deezer": {
		PlatformID: "deezer", DisplayName: "Deezer",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"country"},
		QuantityCol: []string{"nb_of_plays"}, RevenueCol: []string{"royalties"},
		DateStartCol: []string{"start_report"}, DateEndCol: []string{"end_report"},
	},
	"pnd-pandora": {
		PlatformID: "pandora", DisplayName: "Pandora",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"territorycode"},
		QuantityCol: []string{"numberofconsumersalesgross"}, RevenueCol: []string{},
		DateStartCol: []string{"salesdate"}, DateEndCol: []string{},
	},
	"iht-iheart": {
		PlatformID: "iheart", DisplayName: "iHeart",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"country"},
		QuantityCol: []string{"# streams"}, RevenueCol: []string{"price"},
		DateStartCol: []string{"reportstartdt"}, DateEndCol: []string{"reportenddt"},
	},
	"scu-soundcloud": {
		PlatformID: "soundcloud", DisplayName: "SoundCloud",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"territory"},
		QuantityCol: []string{"total plays"}, RevenueCol: []string{"total revenue"},
		DateStartCol: []string{"reporting period"}, DateEndCol: []string{},
	},
	"aum-audiomack": {
		PlatformID: "audiomack", DisplayName: "Audiomack",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"country"},
		QuantityCol: []string{"streams"}, RevenueCol: []string{"total_payable"},
		DateStartCol: []string{"start_date"}, DateEndCol: []string{"end_date"},
	},
	"ang-anghami": {
		PlatformID: "anghami", DisplayName: "Anghami",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"country of sale"},
		QuantityCol: []string{"quantity"}, RevenueCol: []string{"total payable"},
		DateStartCol: []string{"start date"}, DateEndCol: []string{"end date"},
	},
	"awa-awa": {
		PlatformID: "awa", DisplayName: "AWA",
		ISRCCol: []string{"isrc_id"}, CountryCol: []string{"territory_code"},
		QuantityCol: []string{"quantity"}, RevenueCol: []string{"total in usd"},
		DateStartCol: []string{"report date"}, DateEndCol: []string{},
	},
	"tme-tencent": {
		PlatformID: "tencent", DisplayName: "Tencent",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"country_of_sale"},
		QuantityCol: []string{"quantity"}, RevenueCol: []string{"total_payable_usd"},
		DateStartCol: []string{"start_date"}, DateEndCol: []string{"end_date"},
	},
	"svn-saavn": {
		PlatformID: "saavn", DisplayName: "JioSaavn",
		ISRCCol: []string{"isrc id"}, CountryCol: []string{"territory code"},
		QuantityCol: []string{"quantity"}, RevenueCol: []string{"total in usd"},
		DateStartCol: []string{"report date"}, DateEndCol: []string{},
	},
	"tbl-trebel": {
		PlatformID: "trebel", DisplayName: "Trebel",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"territory"},
		QuantityCol: []string{"quantity"}, RevenueCol: []string{"total_payable_usd"},
		DateStartCol: []string{"start_date"}, DateEndCol: []string{"end_date"},
	},
	"stb-soundtrack-your-brand": {
		PlatformID: "soundtrack_your_brand", DisplayName: "Soundtrack Your Brand",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"country_of_sale"},
		QuantityCol: []string{"number_of_transactions"}, RevenueCol: []string{"statement_amount"},
		DateStartCol: []string{"start_date"}, DateEndCol: []string{"end_date"},
	},
	"mxc-mixcloud": {
		PlatformID: "mixcloud", DisplayName: "Mixcloud",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"country_of_sale"},
		QuantityCol: []string{"quantity"}, RevenueCol: []string{"total_payable_usd"},
		DateStartCol: []string{"start_date"}, DateEndCol: []string{"end_date"},
	},
	"ncm-netease-cloud-music": {
		PlatformID: "netease", DisplayName: "NetEase Cloud Music",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"country_of_sale"},
		QuantityCol: []string{"quantity_ streams", "quantity_streams"}, RevenueCol: []string{"usd_amount"},
		DateStartCol: []string{"start_date"}, DateEndCol: []string{"end_date"},
	},
	"vvo-vevo": {
		PlatformID: "vevo", DisplayName: "Vevo",
		ISRCCol: []string{"isrc"}, CountryCol: []string{"territory"},
		QuantityCol: []string{"quantity"}, RevenueCol: []string{"net_revenue"},
		DateStartCol: []string{"start_date"}, DateEndCol: []string{"end_date"},
	},
	"fbk-facebook": {
		PlatformID: "meta", DisplayName: "Meta (Facebook/Instagram/WhatsApp)",
		ISRCCol: []string{"elected_isrc", "isrc"}, CountryCol: []string{"country"},
		QuantityCol: []string{"event_count"}, RevenueCol: []string{},
		DateStartCol: []string{"start_date"}, DateEndCol: []string{"end_date"},
	},
}

// ImportAll imports all Merlin sales data from GCS.
func (m *MerlinSalesImporter) ImportAll(ctx context.Context) (map[string]int, error) {
	results := make(map[string]int)

	// List all month folders
	months, err := m.gcs.ListFolders(ctx, m.bucket, "merlin/sales/")
	if err != nil {
		return nil, fmt.Errorf("list sales months: %w", err)
	}

	m.logger.Info().Int("months", len(months)).Msg("found sales months")

	for _, month := range months {
		// List platform folders in this month
		platforms, err := m.gcs.ListFolders(ctx, m.bucket, month)
		if err != nil {
			continue
		}

		for _, platformDir := range platforms {
			// Extract platform code from path: merlin/sales/202501/spo-spotify/ → spo-spotify
			parts := strings.Split(strings.TrimSuffix(platformDir, "/"), "/")
			platformCode := parts[len(parts)-1]

			mapping, ok := merlinPlatforms[platformCode]
			if !ok {
				continue
			}

			files, err := m.gcs.ListFiles(ctx, m.bucket, platformDir)
			if err != nil {
				continue
			}

			for _, f := range files {
				n, err := m.importFile(ctx, f, mapping)
				if err != nil {
					m.logger.Warn().Err(err).Str("file", f).Str("platform", mapping.PlatformID).Msg("import failed")
					continue
				}
				results[mapping.PlatformID] += n
			}
		}
	}

	return results, nil
}

// ImportPlatform imports data for a specific platform code.
func (m *MerlinSalesImporter) ImportPlatform(ctx context.Context, platformCode string) (int, error) {
	mapping, ok := merlinPlatforms[platformCode]
	if !ok {
		return 0, fmt.Errorf("unknown platform code: %s", platformCode)
	}

	files, err := m.gcs.ListFiles(ctx, m.bucket, fmt.Sprintf("merlin/sales/"))
	if err != nil {
		return 0, err
	}

	total := 0
	for _, f := range files {
		if !strings.Contains(f, platformCode) {
			continue
		}
		n, err := m.importFile(ctx, f, mapping)
		if err != nil {
			m.logger.Warn().Err(err).Str("file", f).Msg("import failed")
			continue
		}
		total += n
	}

	return total, nil
}

func (m *MerlinSalesImporter) importFile(ctx context.Context, filePath string, mapping platformMapping) (int, error) {
	reader, err := m.gcs.ReadFile(ctx, m.bucket, filePath)
	if err != nil {
		return 0, fmt.Errorf("read file: %w", err)
	}

	var csvReader *csv.Reader

	// Handle different file formats
	if strings.HasSuffix(filePath, ".csv.zip") || strings.HasSuffix(filePath, ".zip") {
		data, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			return 0, err
		}
		zipReader, err := ReadZipCSV(data)
		if err != nil {
			return 0, err
		}
		csvReader = csv.NewReader(zipReader)
	} else if strings.HasSuffix(filePath, ".gz") {
		// GCS client auto-decompresses .gz
		csvReader = csv.NewReader(reader)
		defer reader.Close()
	} else if strings.HasSuffix(filePath, ".txt") {
		// TSV files
		csvReader = csv.NewReader(reader)
		csvReader.Comma = '\t'
		defer reader.Close()
	} else {
		csvReader = csv.NewReader(reader)
		defer reader.Close()
	}

	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true
	csvReader.FieldsPerRecord = -1 // variable fields

	// Read header
	header, err := csvReader.Read()
	if err != nil {
		return 0, fmt.Errorf("read header: %w", err)
	}

	// For Spotify and Pandora, the actual data header might be on a later row
	// (they have multi-row headers)
	colIdx := buildColumnIndex(header)
	isrcIdx := findColumn(colIdx, mapping.ISRCCol)

	if isrcIdx < 0 {
		// Try next row (some files have a metadata header first)
		header2, err := csvReader.Read()
		if err != nil {
			return 0, fmt.Errorf("no ISRC column found")
		}
		colIdx = buildColumnIndex(header2)
		isrcIdx = findColumn(colIdx, mapping.ISRCCol)
		if isrcIdx < 0 {
			return 0, fmt.Errorf("no ISRC column in %v", mapping.ISRCCol)
		}
	}

	countryIdx := findColumn(colIdx, mapping.CountryCol)
	quantityIdx := findColumn(colIdx, mapping.QuantityCol)
	revenueIdx := findColumn(colIdx, mapping.RevenueCol)
	dateStartIdx := findColumn(colIdx, mapping.DateStartCol)

	// Aggregate by (ISRC, country, month) to avoid duplicate key issues
	type aggKey struct {
		ISRC    string
		Country string
		Date    string
	}
	streamAgg := make(map[aggKey]int64)
	revenueAgg := make(map[aggKey]float64)

	// Determine the report month from filename if date columns aren't useful
	reportDate := extractDateFromPath(filePath)

	for {
		row, err := csvReader.Read()
		if err != nil {
			break
		}

		isrc := getCSVFieldSafe(row, isrcIdx)
		if isrc == "" || len(isrc) < 10 {
			continue
		}

		country := ""
		if countryIdx >= 0 {
			country = getCSVFieldSafe(row, countryIdx)
		}

		// Parse date — use report date if per-row date not available
		dateStr := reportDate
		if dateStartIdx >= 0 {
			d := getCSVFieldSafe(row, dateStartIdx)
			if d != "" {
				dateStr = normalizeDate(d)
			}
		}

		quantity := int64(0)
		if quantityIdx >= 0 {
			quantity = parseIntLoose(getCSVFieldSafe(row, quantityIdx))
		}

		revenue := 0.0
		if revenueIdx >= 0 {
			revenue = parseFloatLoose(getCSVFieldSafe(row, revenueIdx))
		}

		k := aggKey{ISRC: isrc, Country: country, Date: dateStr}
		streamAgg[k] += quantity
		revenueAgg[k] += revenue
	}

	// Build upserts
	var metricUpserts []model.MetricUpsert
	var revenueUpserts []model.RevenueUpsert

	for k, streams := range streamAgg {
		if streams == 0 && revenueAgg[k] == 0 {
			continue
		}

		assetID, err := m.metricsRepo.ResolveAssetID(ctx, "isrc", k.ISRC)
		if err != nil || assetID == nil {
			continue
		}

		date, _ := time.Parse("2006-01-02", k.Date)
		if date.IsZero() {
			continue
		}

		var territory *string
		if k.Country != "" && len(k.Country) == 2 {
			territory = &k.Country
		}

		if streams > 0 {
			metricUpserts = append(metricUpserts, model.MetricUpsert{
				AssetID:    *assetID,
				PlatformID: mapping.PlatformID,
				Territory:  territory,
				MetricDate: date,
				MetricType: model.MetricStreams,
				Value:      streams,
			})
		}

		if rev := revenueAgg[k]; rev > 0 {
			revenueUpserts = append(revenueUpserts, model.RevenueUpsert{
				AssetID:     *assetID,
				PlatformID:  mapping.PlatformID,
				Territory:   territory,
				RevenueDate: date,
				RevenueType: model.RevenueAdSupported,
				Currency:    "USD",
				Amount:      rev,
				AmountUSD:   &rev,
			})
		}
	}

	inserted := 0
	if len(metricUpserts) > 0 {
		ins, _, err := m.metricsRepo.BulkUpsert(ctx, metricUpserts)
		if err != nil {
			return 0, fmt.Errorf("bulk upsert metrics: %w", err)
		}
		inserted += ins
	}

	if len(revenueUpserts) > 0 && m.revenueRepo != nil {
		ins, _, err := m.revenueRepo.BulkUpsert(ctx, revenueUpserts)
		if err != nil {
			m.logger.Warn().Err(err).Str("file", filePath).Msg("revenue upsert failed (non-fatal)")
		} else {
			inserted += ins
		}
	}

	if inserted > 0 {
		m.logger.Info().Str("file", filePath).Str("platform", mapping.PlatformID).Int("metrics", len(metricUpserts)).Int("revenue", len(revenueUpserts)).Msg("file imported")
	}

	return inserted, nil
}

// ListFolders lists immediate subdirectories of a prefix.
func (g *GCSClient) ListFolders(ctx context.Context, bucket, prefix string) ([]string, error) {
	files, err := g.ListFiles(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}

	folderSet := make(map[string]bool)
	for _, f := range files {
		rel := strings.TrimPrefix(f, prefix)
		parts := strings.SplitN(rel, "/", 2)
		if len(parts) >= 2 {
			folderSet[prefix+parts[0]+"/"] = true
		}
	}

	folders := make([]string, 0, len(folderSet))
	for f := range folderSet {
		folders = append(folders, f)
	}
	return folders, nil
}

// -- Helpers --

func buildColumnIndex(header []string) map[string]int {
	idx := make(map[string]int)
	for i, col := range header {
		cleaned := strings.ToLower(strings.TrimSpace(strings.Trim(col, "\ufeff\"")))
		idx[cleaned] = i
	}
	return idx
}

func findColumn(colIdx map[string]int, candidates []string) int {
	for _, name := range candidates {
		if idx, ok := colIdx[name]; ok {
			return idx
		}
	}
	return -1
}

func getCSVFieldSafe(row []string, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(strings.Trim(row[idx], "\""))
}

func parseIntLoose(s string) int64 {
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)
	n, _ := strconv.ParseInt(s, 10, 64)
	if n == 0 {
		f, _ := strconv.ParseFloat(s, 64)
		n = int64(f)
	}
	return n
}

func parseFloatLoose(s string) float64 {
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// normalizeDate attempts to parse various date formats into YYYY-MM-DD.
func normalizeDate(s string) string {
	s = strings.TrimSpace(s)
	formats := []string{
		"2006-01-02",
		"01/02/2006",
		"02-01-2006",
		"20060102",
		"01/31/2006",
		"2006/1/2",
		"2006/01/02",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t.Format("2006-01-02")
		}
	}
	// Try month-only format like "01-2025"
	if t, err := time.Parse("01-2025", s); err == nil {
		return t.Format("2006-01-02")
	}
	return s
}

// extractDateFromPath extracts a YYYYMM date from the file path and returns first of month.
func extractDateFromPath(path string) string {
	// Look for 6-digit month pattern like 202501
	for _, part := range strings.Split(path, "/") {
		if len(part) == 6 {
			if _, err := strconv.Atoi(part); err == nil {
				year := part[:4]
				month := part[4:6]
				return fmt.Sprintf("%s-%s-01", year, month)
			}
		}
	}
	return ""
}

// ReadZipCSV is already defined in apple.go — this wraps it for reuse
func readZipToReader(data []byte) (io.Reader, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	if len(zr.File) == 0 {
		return nil, fmt.Errorf("empty zip")
	}
	return zr.File[0].Open()
}
