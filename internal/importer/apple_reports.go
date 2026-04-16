package importer

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/mattercollective/analytics-engine/internal/model"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

// AppleReportsImporter imports Apple Music reports from GCS:
// - Editorial Playlist Adds (ameditorialplaylistadds)
// - Content Demographics (amcontentdemographics)
// - Shazams (amshazam)
// - Container/Source of Stream (amcontainer)
type AppleReportsImporter struct {
	gcs            *GCSClient
	metricsRepo    *repository.MetricsRepo
	playlistRepo   *repository.PlaylistRepo
	engagementRepo *repository.EngagementRepo
	logger         zerolog.Logger
	bucket         string

	appleIDToISRC map[string]string
}

func NewAppleReportsImporter(
	gcs *GCSClient,
	metricsRepo *repository.MetricsRepo,
	playlistRepo *repository.PlaylistRepo,
	engagementRepo *repository.EngagementRepo,
	logger zerolog.Logger,
	bucket string,
) *AppleReportsImporter {
	return &AppleReportsImporter{
		gcs:            gcs,
		metricsRepo:    metricsRepo,
		playlistRepo:   playlistRepo,
		engagementRepo: engagementRepo,
		logger:         logger.With().Str("importer", "apple_reports").Logger(),
		bucket:         bucket,
		appleIDToISRC:  make(map[string]string),
	}
}

// LoadContentMapping loads the Apple Identifier → ISRC mapping from the content file.
func (a *AppleReportsImporter) LoadContentMapping(ctx context.Context, contentPath string) error {
	a.logger.Info().Str("path", contentPath).Msg("loading Apple content mapping")

	reader, err := a.gcs.ReadFile(ctx, a.bucket, contentPath)
	if err != nil {
		return fmt.Errorf("read content file: %w", err)
	}
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	if !scanner.Scan() {
		return fmt.Errorf("empty content file")
	}
	colIdx := parseTSVHeader(scanner.Text())
	appleIDIdx, ok1 := colIdx["apple identifier"]
	isrcIdx, ok2 := colIdx["isrc"]
	if !ok1 || !ok2 {
		return fmt.Errorf("content file missing Apple Identifier or ISRC column")
	}

	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		appleID := getField(fields, appleIDIdx)
		isrc := getField(fields, isrcIdx)
		if appleID != "" && isrc != "" {
			a.appleIDToISRC[appleID] = isrc
		}
	}

	a.logger.Info().Int("mappings", len(a.appleIDToISRC)).Msg("content mapping loaded")
	return nil
}

// ImportEditorialPlaylistAdds imports the editorial playlist adds report.
// Columns: Datestamp, Storefront Name, Apple Identifier, Title, Artist, Artist ID,
// Media Type, Action Type, Playlist ID, Playlist Name, Track Position
func (a *AppleReportsImporter) ImportEditorialPlaylistAdds(ctx context.Context, prefix string) (int, error) {
	files, err := a.gcs.ListFiles(ctx, a.bucket, prefix)
	if err != nil {
		return 0, fmt.Errorf("list files: %w", err)
	}

	totalPositions := 0

	for _, f := range files {
		if !strings.HasSuffix(f, ".txt") {
			continue
		}

		reader, err := a.gcs.ReadFile(ctx, a.bucket, f)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

		if !scanner.Scan() {
			reader.Close()
			continue
		}
		colIdx := parseTSVHeader(scanner.Text())

		var positions []model.PlaylistPosition
		playlistCache := make(map[string]bool) // track which playlists we've upserted

		for scanner.Scan() {
			fields := strings.Split(scanner.Text(), "\t")

			dateStr := getField(fields, colIdx["datestamp"])
			storefront := getField(fields, colIdx["storefront name"])
			appleID := getField(fields, colIdx["apple identifier"])
			title := getField(fields, colIdx["title"])
			artist := getField(fields, colIdx["artist"])
			playlistID := getField(fields, colIdx["playlist id"])
			playlistName := getField(fields, colIdx["playlist name"])
			posStr := getField(fields, colIdx["track position"])

			if appleID == "" || playlistID == "" {
				continue
			}

			// Resolve Apple ID → ISRC → asset_id
			isrc := a.appleIDToISRC[appleID]
			if isrc == "" {
				continue
			}

			assetID, err := a.metricsRepo.ResolveAssetID(ctx, "isrc", isrc)
			if err != nil || assetID == nil {
				continue
			}

			// Upsert playlist metadata
			if !playlistCache[playlistID] {
				curatorType := "editorial"
				a.playlistRepo.UpsertPlaylist(ctx, model.PlaylistUpsert{
					PlatformID:  "apple_music",
					ExternalID:  playlistID,
					Name:        playlistName,
					CuratorType: &curatorType,
				})
				playlistCache[playlistID] = true
			}

			// Get internal playlist UUID
			// For now, look it up by external_id
			date, _ := time.Parse("2006-01-02", dateStr)
			pos, _ := strconv.Atoi(posStr)

			_ = storefront
			_ = title
			_ = artist

			positions = append(positions, model.PlaylistPosition{
				AssetID:      *assetID,
				SnapshotDate: date,
				Position:     &pos,
			})
		}
		reader.Close()

		// We need playlist UUIDs for the positions — batch resolve
		if len(positions) > 0 {
			// For now, store as playlist_adds metrics
			var upserts []model.MetricUpsert
			for _, p := range positions {
				upserts = append(upserts, model.MetricUpsert{
					AssetID:    p.AssetID,
					PlatformID: "apple_music",
					MetricDate: p.SnapshotDate,
					MetricType: model.MetricPlaylistAdds,
					Value:      1,
				})
			}

			ins, _, err := a.metricsRepo.BulkUpsert(ctx, upserts)
			if err != nil {
				a.logger.Error().Err(err).Str("file", f).Msg("editorial playlist upsert failed")
				continue
			}
			totalPositions += ins
		}

		a.logger.Info().Str("file", f).Int("rows", len(positions)).Msg("editorial playlist file imported")
	}

	a.logger.Info().Int("total", totalPositions).Msg("editorial playlist adds import complete")
	return totalPositions, nil
}

// ImportDemographics imports the content demographics report.
// Columns: Start Date, End Date, Apple Identifier, Storefront Name,
// Subscription Type, Subscription Mode, Action Type, Gender, Age Band,
// Audio Format, Listeners, Streams
func (a *AppleReportsImporter) ImportDemographics(ctx context.Context, prefix string) (int, error) {
	files, err := a.gcs.ListFiles(ctx, a.bucket, prefix)
	if err != nil {
		return 0, fmt.Errorf("list files: %w", err)
	}

	totalRows := 0

	for _, f := range files {
		if !strings.HasSuffix(f, ".txt") {
			continue
		}

		reader, err := a.gcs.ReadFile(ctx, a.bucket, f)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

		if !scanner.Scan() {
			reader.Close()
			continue
		}
		colIdx := parseTSVHeader(scanner.Text())

		// Pre-aggregate by (assetID, storefront, date, ageBucket, gender)
		// because Apple breaks out by subscription type which we don't need
		type demoKey struct {
			AssetID   string
			Territory string
			Date      string
			AgeBucket string
			Gender    string
		}
		agg := make(map[demoKey]*model.DemographicUpsert)

		for scanner.Scan() {
			fields := strings.Split(scanner.Text(), "\t")

			startDate := getField(fields, colIdx["start date"])
			appleID := getField(fields, colIdx["apple identifier"])
			storefront := getField(fields, colIdx["storefront name"])
			genderCode := getField(fields, colIdx["gender"])
			ageBand := getField(fields, colIdx["age band"])
			listenersStr := getField(fields, colIdx["listeners"])
			streamsStr := getField(fields, colIdx["streams"])

			if appleID == "" {
				continue
			}

			isrc := a.appleIDToISRC[appleID]
			if isrc == "" {
				continue
			}

			assetID, err := a.metricsRepo.ResolveAssetID(ctx, "isrc", isrc)
			if err != nil || assetID == nil {
				continue
			}

			listeners, _ := strconv.ParseInt(listenersStr, 10, 64)
			streams, _ := strconv.ParseInt(streamsStr, 10, 64)
			mappedAge := mapAppleAgeBand(ageBand)
			mappedGender := mapAppleGender(genderCode)

			k := demoKey{
				AssetID:   assetID.String(),
				Territory: storefront,
				Date:      startDate,
				AgeBucket: mappedAge,
				Gender:    mappedGender,
			}

			if existing, ok := agg[k]; ok {
				existing.Streams += streams
				existing.Listeners += listeners
			} else {
				date, _ := time.Parse("2006-01-02", startDate)
				agg[k] = &model.DemographicUpsert{
					AssetID:    *assetID,
					PlatformID: "apple_music",
					Territory:  &storefront,
					DemoDate:   date,
					AgeBucket:  mappedAge,
					Gender:     mappedGender,
					Streams:    streams,
					Listeners:  listeners,
				}
			}
		}
		reader.Close()

		demos := make([]model.DemographicUpsert, 0, len(agg))
		for _, d := range agg {
			demos = append(demos, *d)
		}

		if len(demos) > 0 {
			n, err := a.engagementRepo.BulkUpsertDemographics(ctx, demos)
			if err != nil {
				a.logger.Error().Err(err).Str("file", f).Msg("demographics upsert failed")
				continue
			}
			totalRows += n
		}

		a.logger.Info().Str("file", f).Int("rows", len(demos)).Msg("demographics file imported")
	}

	a.logger.Info().Int("total", totalRows).Msg("demographics import complete")
	return totalRows, nil
}

// ImportShazams imports the daily Shazam report.
// Columns: Ingest Datestamp, Ingest Hour, Title, Artist, Country,
// State Province, City, ISRC Codes, Shazams
func (a *AppleReportsImporter) ImportShazams(ctx context.Context, prefix string) (int, error) {
	files, err := a.gcs.ListFiles(ctx, a.bucket, prefix)
	if err != nil {
		return 0, fmt.Errorf("list files: %w", err)
	}

	totalRows := 0

	for _, f := range files {
		if !strings.HasSuffix(f, ".txt") {
			continue
		}

		reader, err := a.gcs.ReadFile(ctx, a.bucket, f)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

		if !scanner.Scan() {
			reader.Close()
			continue
		}
		colIdx := parseTSVHeader(scanner.Text())

		// Aggregate Shazams by (ISRC, date, country) since the file has hourly rows
		type shazamKey struct {
			ISRC    string
			Date    string
			Country string
		}
		agg := make(map[shazamKey]int64)

		for scanner.Scan() {
			fields := strings.Split(scanner.Text(), "\t")

			dateStr := getField(fields, colIdx["ingest datestamp"])
			country := getField(fields, colIdx["country"])
			isrcCodes := getField(fields, colIdx["isrc codes"])
			shazamsStr := getField(fields, colIdx["shazams"])

			if isrcCodes == "" {
				continue
			}

			shazams, _ := strconv.ParseInt(shazamsStr, 10, 64)
			if shazams == 0 {
				continue
			}

			// ISRC Codes may be comma-separated for multiple ISRCs
			for _, isrc := range strings.Split(isrcCodes, ",") {
				isrc = strings.TrimSpace(isrc)
				if isrc != "" {
					k := shazamKey{ISRC: isrc, Date: dateStr, Country: country}
					agg[k] += shazams
				}
			}
		}
		reader.Close()

		var upserts []model.MetricUpsert
		for k, count := range agg {
			assetID, err := a.metricsRepo.ResolveAssetID(ctx, "isrc", k.ISRC)
			if err != nil || assetID == nil {
				continue
			}

			date, _ := time.Parse("2006-01-02", k.Date)
			u := model.MetricUpsert{
				AssetID:    *assetID,
				PlatformID: "apple_music",
				MetricDate: date,
				MetricType: model.MetricShazams,
				Value:      count,
			}
			if k.Country != "" {
				u.Territory = &k.Country
			}
			upserts = append(upserts, u)
		}

		if len(upserts) > 0 {
			ins, _, err := a.metricsRepo.BulkUpsert(ctx, upserts)
			if err != nil {
				a.logger.Error().Err(err).Str("file", f).Msg("shazam upsert failed")
				continue
			}
			totalRows += ins
		}

		a.logger.Info().Str("file", f).Int("aggregated", len(upserts)).Msg("shazam file imported")
	}

	a.logger.Info().Int("total", totalRows).Msg("shazam import complete")
	return totalRows, nil
}

// ImportContainers imports the container/source-of-stream report.
// Columns: Ingest Datestamp, Storefront Name, Apple Identifier,
// Container Type, Container Sub-Type, Container ID, Container Name, Streams
func (a *AppleReportsImporter) ImportContainers(ctx context.Context, prefix string) (int, error) {
	files, err := a.gcs.ListFiles(ctx, a.bucket, prefix)
	if err != nil {
		return 0, fmt.Errorf("list files: %w", err)
	}

	totalRows := 0

	for _, f := range files {
		if !strings.HasSuffix(f, ".txt") {
			continue
		}

		reader, err := a.gcs.ReadFile(ctx, a.bucket, f)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

		if !scanner.Scan() {
			reader.Close()
			continue
		}
		colIdx := parseTSVHeader(scanner.Text())

		var engagements []model.EngagementUpsert

		for scanner.Scan() {
			fields := strings.Split(scanner.Text(), "\t")

			dateStr := getField(fields, colIdx["ingest datestamp"])
			storefront := getField(fields, colIdx["storefront name"])
			appleID := getField(fields, colIdx["apple identifier"])
			containerType := getField(fields, colIdx["container type"])
			containerSubType := getField(fields, colIdx["container sub-type"])
			containerID := getField(fields, colIdx["container id"])
			containerName := getField(fields, colIdx["container name"])
			streamsStr := getField(fields, colIdx["streams"])

			if appleID == "" {
				continue
			}

			isrc := a.appleIDToISRC[appleID]
			if isrc == "" {
				continue
			}

			assetID, err := a.metricsRepo.ResolveAssetID(ctx, "isrc", isrc)
			if err != nil || assetID == nil {
				continue
			}

			date, _ := time.Parse("2006-01-02", dateStr)
			streams, _ := strconv.ParseInt(streamsStr, 10, 64)
			if streams == 0 {
				continue
			}

			source := mapAppleContainerType(containerType, containerSubType)
			sourceURI := fmt.Sprintf("apple:%s:%s", containerID, containerName)

			_ = storefront // TODO: use as territory when we have ISO code mapping

			engagements = append(engagements, model.EngagementUpsert{
				AssetID:        *assetID,
				PlatformID:     "apple_music",
				EngagementDate: date,
				Source:         source,
				SourceURI:      &sourceURI,
				Streams:        streams,
			})
		}
		reader.Close()

		if len(engagements) > 0 {
			n, err := a.engagementRepo.BulkUpsertEngagement(ctx, engagements)
			if err != nil {
				a.logger.Error().Err(err).Str("file", f).Msg("container upsert failed")
				continue
			}
			totalRows += n
		}

		a.logger.Info().Str("file", f).Int("rows", len(engagements)).Msg("container file imported")
	}

	a.logger.Info().Int("total", totalRows).Msg("container import complete")
	return totalRows, nil
}

// -- Apple mapping helpers --

func mapAppleGender(code string) string {
	switch code {
	case "1":
		return "male"
	case "2":
		return "female"
	case "3":
		return "non_binary"
	default:
		return "unknown"
	}
}

func mapAppleAgeBand(band string) string {
	// Apple uses: "Under 18", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"
	switch band {
	case "Under 18":
		return "13-17"
	default:
		return band
	}
}

// mapAppleContainerType maps Apple container type codes to our standard source names.
// Container Type 1 = Browse, 2 = Playlist, 3 = Radio, 4 = Search, 5 = Siri, etc.
func mapAppleContainerType(containerType, subType string) string {
	switch containerType {
	case "1":
		return "browse"
	case "2":
		return "playlist"
	case "3":
		return "radio"
	case "4":
		return "search"
	case "5":
		return "siri"
	case "6":
		return "library"
	case "7":
		return "artist_page"
	default:
		return "other"
	}
}
