package importer

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

type trackInfo struct {
	ISRC   string
	UPC    string
	Title  string
	Artist string
}

// cmsRef links an ISRC to an existing YouTube asset and client in the CMS.
type cmsRef struct {
	CMSAssetID string // YouTube asset ID like A556642548909115
	OrgID      string // client UUID
}

// AssetSeeder extracts unique ISRCs from report files and creates
// asset + asset_identifier records for any that don't already exist.
// When a CMS map is loaded via LoadCMSMap, the seeder will attach ISRCs
// to existing YouTube assets instead of creating duplicates.
type AssetSeeder struct {
	gcs    *GCSClient
	pool   *pgxpool.Pool
	logger zerolog.Logger
	bucket string

	cmsISRCMap map[string]cmsRef // optional: ISRC → (CMS asset_id, org)
}

func NewAssetSeeder(gcs *GCSClient, pool *pgxpool.Pool, logger zerolog.Logger, bucket string) *AssetSeeder {
	return &AssetSeeder{
		gcs:    gcs,
		pool:   pool,
		logger: logger.With().Str("component", "seeder").Logger(),
		bucket: bucket,
	}
}

// SeedFromAmazon extracts ISRCs from Amazon CSV files and creates assets.
func (s *AssetSeeder) SeedFromAmazon(ctx context.Context, prefix string) (int, error) {
	files, err := s.gcs.ListFiles(ctx, s.bucket, prefix)
	if err != nil {
		return 0, fmt.Errorf("list files: %w", err)
	}

	tracks := make(map[string]trackInfo)

	for _, f := range files {
		if !strings.HasSuffix(f, ".csv") {
			continue
		}

		reader, err := s.gcs.ReadFile(ctx, s.bucket, f)
		if err != nil {
			continue
		}

		csvReader := csv.NewReader(reader)
		csvReader.LazyQuotes = true
		csvReader.TrimLeadingSpace = true

		header, err := csvReader.Read()
		if err != nil {
			reader.Close()
			continue
		}
		colIdx := parseCSVHeader(header)

		isrcIdx, hasISRC := colIdx["isrc"]
		upcIdx, hasUPC := colIdx["upc"]
		trackIdx, hasTrack := colIdx["track_name"]
		artistIdx, hasArtist := colIdx["artist_name"]

		if !hasISRC {
			reader.Close()
			continue
		}

		for {
			row, err := csvReader.Read()
			if err != nil {
				break
			}

			isrc := getCSVField(row, isrcIdx)
			if isrc == "" {
				continue
			}

			if _, exists := tracks[isrc]; !exists {
				info := trackInfo{ISRC: isrc}
				if hasUPC {
					info.UPC = getCSVField(row, upcIdx)
				}
				if hasTrack {
					info.Title = getCSVField(row, trackIdx)
				}
				if hasArtist {
					info.Artist = getCSVField(row, artistIdx)
				}
				tracks[isrc] = info
			}
		}
		reader.Close()
	}

	s.logger.Info().Int("unique_isrcs", len(tracks)).Msg("extracted ISRCs from Amazon files")
	return s.insertTracks(ctx, tracks)
}

// SeedFromMerlin extracts ISRCs from Merlin trends zipped CSV files.
func (s *AssetSeeder) SeedFromMerlin(ctx context.Context, prefix string) (int, error) {
	files, err := s.gcs.ListFiles(ctx, s.bucket, prefix)
	if err != nil {
		return 0, fmt.Errorf("list files: %w", err)
	}

	tracks := make(map[string]trackInfo)

	for _, f := range files {
		if !strings.HasSuffix(f, ".csv.zip") && !strings.HasSuffix(f, ".zip") {
			continue
		}

		rawReader, err := s.gcs.ReadFile(ctx, s.bucket, f)
		if err != nil {
			continue
		}
		data, err := io.ReadAll(rawReader)
		rawReader.Close()
		if err != nil {
			continue
		}

		zipReader, err := ReadZipCSV(data)
		if err != nil {
			continue
		}

		csvReader := csv.NewReader(zipReader)
		csvReader.LazyQuotes = true
		csvReader.TrimLeadingSpace = true

		header, err := csvReader.Read()
		if err != nil {
			continue
		}
		colIdx := parseCSVHeader(header)

		isrcIdx, hasISRC := colIdx["isrc"]
		trackIdx, hasTrack := colIdx["track_name"]
		artistIdx, hasArtist := colIdx["artists"]

		if !hasISRC {
			continue
		}

		for {
			row, err := csvReader.Read()
			if err != nil {
				break
			}

			isrc := getCSVField(row, isrcIdx)
			if isrc == "" {
				continue
			}

			if _, exists := tracks[isrc]; !exists {
				info := trackInfo{ISRC: isrc}
				if hasTrack {
					info.Title = getCSVField(row, trackIdx)
				}
				if hasArtist {
					info.Artist = getCSVField(row, artistIdx)
				}
				tracks[isrc] = info
			}
		}
	}

	s.logger.Info().Int("unique_isrcs", len(tracks)).Msg("extracted ISRCs from Merlin files")
	return s.insertTracks(ctx, tracks)
}

// SeedFromApple extracts ISRCs from the Apple content file and creates assets.
func (s *AssetSeeder) SeedFromApple(ctx context.Context, contentPath string) (int, error) {
	reader, err := s.gcs.ReadFile(ctx, s.bucket, contentPath)
	if err != nil {
		return 0, fmt.Errorf("read content file: %w", err)
	}
	defer reader.Close()

	tracks := make(map[string]trackInfo)

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	if !scanner.Scan() {
		return 0, fmt.Errorf("empty content file")
	}
	colIdx := parseTSVHeader(scanner.Text())

	isrcIdx, ok1 := colIdx["isrc"]
	titleIdx, ok2 := colIdx["title"]
	artistIdx, ok3 := colIdx["artist"]
	if !ok1 {
		return 0, fmt.Errorf("content file missing ISRC column")
	}

	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		isrc := getField(fields, isrcIdx)
		if isrc == "" {
			continue
		}

		if _, exists := tracks[isrc]; !exists {
			info := trackInfo{ISRC: isrc}
			if ok2 {
				info.Title = getField(fields, titleIdx)
			}
			if ok3 {
				info.Artist = getField(fields, artistIdx)
			}
			tracks[isrc] = info
		}
	}

	s.logger.Info().Int("unique_isrcs", len(tracks)).Msg("extracted ISRCs from Apple content file")
	return s.insertTracks(ctx, tracks)
}

// LoadCMSMap pulls ISRC→(CMS asset_id, org) mappings from the CMS Supabase and
// caches them in memory. When set, insertTracks will attach ISRCs to existing
// YouTube assets instead of creating duplicates.
func (s *AssetSeeder) LoadCMSMap(ctx context.Context, cmsURL, cmsKey string) error {
	type cmsRow struct {
		AssetID      string `json:"asset_id"`
		DisplayISRC  string `json:"display_isrc"`
		YourISRC     string `json:"your_isrc"`
		OtherISRC    string `json:"other_isrc"`
		Organization string `json:"organization"`
	}

	var rows []cmsRow
	offset := 0
	limit := 1000

	for {
		u := fmt.Sprintf("%s/rest/v1/asset_reports_with_org?select=asset_id,display_isrc,your_isrc,other_isrc,organization&display_isrc=not.is.null&limit=%d&offset=%d",
			cmsURL, limit, offset)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		req.Header.Set("apikey", cmsKey)
		req.Header.Set("Authorization", "Bearer "+cmsKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var page []cmsRow
		if err := json.Unmarshal(body, &page); err != nil {
			return err
		}
		rows = append(rows, page...)
		if len(page) < limit {
			break
		}
		offset += limit
	}

	s.cmsISRCMap = make(map[string]cmsRef, len(rows))
	for _, row := range rows {
		if row.AssetID == "" {
			continue
		}
		for _, isrc := range append([]string{row.DisplayISRC, row.YourISRC}, strings.Fields(row.OtherISRC)...) {
			isrc = strings.TrimSpace(isrc)
			if len(isrc) >= 10 {
				if _, exists := s.cmsISRCMap[isrc]; !exists {
					s.cmsISRCMap[isrc] = cmsRef{CMSAssetID: row.AssetID, OrgID: row.Organization}
				}
			}
		}
	}

	s.logger.Info().Int("isrcs_cached", len(s.cmsISRCMap)).Msg("loaded CMS ISRC map")
	return nil
}

// insertTracks creates asset + asset_identifier records for tracks that don't already exist.
// When CMS map is loaded, attaches ISRCs to existing YouTube assets instead of creating duplicates.
func (s *AssetSeeder) insertTracks(ctx context.Context, tracks map[string]trackInfo) (int, error) {
	created := 0
	attached := 0
	mapped := 0

	for _, info := range tracks {
		var exists bool
		err := s.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM public.asset_identifiers WHERE identifier_type = 'isrc' AND identifier_value = $1 AND effective_to IS NULL)`,
			info.ISRC,
		).Scan(&exists)
		if err != nil {
			continue
		}

		// Case 1: ISRC already resolves to an asset — nothing to do
		if exists {
			continue
		}

		// Case 2: CMS map says this ISRC belongs to an existing YouTube asset — attach instead of creating
		if ref, ok := s.cmsISRCMap[info.ISRC]; ok && ref.CMSAssetID != "" {
			var ytAssetUUID string
			err := s.pool.QueryRow(ctx,
				`SELECT asset_id FROM public.asset_identifiers
				 WHERE identifier_type = 'yt_asset_id' AND identifier_value = $1 AND effective_to IS NULL
				 LIMIT 1`,
				ref.CMSAssetID,
			).Scan(&ytAssetUUID)

			if err == nil {
				// Attach ISRC to existing YouTube asset
				_, _ = s.pool.Exec(ctx,
					`INSERT INTO public.asset_identifiers (asset_id, identifier_type, identifier_value, source)
					 VALUES ($1, 'isrc', $2, 'gcs_import_cms_merge') ON CONFLICT DO NOTHING`,
					ytAssetUUID, info.ISRC,
				)
				if info.UPC != "" {
					s.pool.Exec(ctx,
						`INSERT INTO public.asset_identifiers (asset_id, identifier_type, identifier_value, source)
						 VALUES ($1, 'upc', $2, 'gcs_import_cms_merge') ON CONFLICT DO NOTHING`,
						ytAssetUUID, info.UPC,
					)
				}
				if info.Artist != "" {
					s.pool.Exec(ctx,
						`UPDATE public.assets SET artist_name = $1 WHERE id = $2 AND artist_name IS NULL`,
						info.Artist, ytAssetUUID,
					)
				}
				// Ensure client_assets mapping
				if ref.OrgID != "" {
					if tag, err := s.pool.Exec(ctx,
						`INSERT INTO analytics.client_assets (client_id, asset_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
						ref.OrgID, ytAssetUUID,
					); err == nil && tag.RowsAffected() > 0 {
						mapped++
					}
				}
				attached++
				continue
			}
		}

		// Case 3: No existing asset anywhere — create new ISRC-based asset
		title := info.Title
		if title == "" {
			title = "Unknown Track"
		}
		if info.Artist != "" {
			title = info.Title + " - " + info.Artist
		}

		tx, err := s.pool.Begin(ctx)
		if err != nil {
			continue
		}

		var artistName *string
		if info.Artist != "" {
			artistName = &info.Artist
		}

		var assetID string
		err = tx.QueryRow(ctx,
			`INSERT INTO public.assets (asset_type, title, artist_name) VALUES ('track', $1, $2) RETURNING id`,
			title, artistName,
		).Scan(&assetID)
		if err != nil {
			tx.Rollback(ctx)
			continue
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO public.asset_identifiers (asset_id, identifier_type, identifier_value, source)
			 VALUES ($1, 'isrc', $2, 'gcs_import') ON CONFLICT DO NOTHING`,
			assetID, info.ISRC,
		)
		if err != nil {
			tx.Rollback(ctx)
			continue
		}

		if info.UPC != "" {
			tx.Exec(ctx,
				`INSERT INTO public.asset_identifiers (asset_id, identifier_type, identifier_value, source)
				 VALUES ($1, 'upc', $2, 'gcs_import') ON CONFLICT DO NOTHING`,
				assetID, info.UPC,
			)
		}

		// If CMS map has an org for this ISRC (but no YT asset in our DB), still create the mapping
		if ref, ok := s.cmsISRCMap[info.ISRC]; ok && ref.OrgID != "" {
			tx.Exec(ctx,
				`INSERT INTO analytics.client_assets (client_id, asset_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
				ref.OrgID, assetID,
			)
			mapped++
		}

		if err := tx.Commit(ctx); err != nil {
			continue
		}

		created++
		if (created+attached)%500 == 0 {
			s.logger.Info().Int("created", created).Int("attached", attached).Int("mapped", mapped).Msg("seeding progress")
		}
	}

	s.logger.Info().Int("created", created).Int("attached_to_cms", attached).Int("client_mappings", mapped).Msg("asset seeding complete")
	return created, nil
}

// BackfillArtistNames reads report files and updates artist_name on existing assets
// where it is currently NULL. Returns the number of assets updated.
func (s *AssetSeeder) BackfillArtistNames(ctx context.Context, applePath, amazonPrefix, merlinPrefix string) (int, error) {
	// Collect ISRC → artist mappings from all sources
	artists := make(map[string]string) // ISRC → artist name

	// Apple content file
	if applePath != "" {
		s.logger.Info().Str("path", applePath).Msg("reading Apple content for artist names")
		reader, err := s.gcs.ReadFile(ctx, s.bucket, applePath)
		if err == nil {
			scanner := bufio.NewScanner(reader)
			scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

			if scanner.Scan() {
				colIdx := parseTSVHeader(scanner.Text())
				isrcIdx, ok1 := colIdx["isrc"]
				artistIdx, ok2 := colIdx["artist"]

				if ok1 && ok2 {
					for scanner.Scan() {
						fields := strings.Split(scanner.Text(), "\t")
						isrc := getField(fields, isrcIdx)
						artist := getField(fields, artistIdx)
						if isrc != "" && artist != "" {
							artists[isrc] = artist
						}
					}
				}
			}
			reader.Close()
			s.logger.Info().Int("mappings", len(artists)).Msg("Apple artist mappings loaded")
		}
	}

	// Amazon CSVs
	if amazonPrefix != "" {
		s.logger.Info().Str("prefix", amazonPrefix).Msg("reading Amazon files for artist names")
		files, err := s.gcs.ListFiles(ctx, s.bucket, amazonPrefix)
		if err == nil {
			for _, f := range files {
				if !strings.HasSuffix(f, ".csv") {
					continue
				}
				reader, err := s.gcs.ReadFile(ctx, s.bucket, f)
				if err != nil {
					continue
				}
				csvReader := csv.NewReader(reader)
				csvReader.LazyQuotes = true
				csvReader.TrimLeadingSpace = true

				header, err := csvReader.Read()
				if err != nil {
					reader.Close()
					continue
				}
				colIdx := parseCSVHeader(header)
				isrcIdx, hasISRC := colIdx["isrc"]
				artistIdx, hasArtist := colIdx["artist_name"]

				if hasISRC && hasArtist {
					for {
						row, err := csvReader.Read()
						if err != nil {
							break
						}
						isrc := getCSVField(row, isrcIdx)
						artist := getCSVField(row, artistIdx)
						if isrc != "" && artist != "" {
							if _, exists := artists[isrc]; !exists {
								artists[isrc] = artist
							}
						}
					}
				}
				reader.Close()
			}
			s.logger.Info().Int("mappings", len(artists)).Msg("artist mappings after Amazon")
		}
	}

	// Merlin zipped TSVs
	if merlinPrefix != "" {
		s.logger.Info().Str("prefix", merlinPrefix).Msg("reading Merlin files for artist names")
		files, err := s.gcs.ListFiles(ctx, s.bucket, merlinPrefix)
		if err == nil {
			sampled := 0
			for _, f := range files {
				if !strings.HasSuffix(f, ".zip") {
					continue
				}
				// Sample every 30th file to avoid reading all 3K+ files
				sampled++
				if sampled%30 != 0 {
					continue
				}

				rawReader, err := s.gcs.ReadFile(ctx, s.bucket, f)
				if err != nil {
					continue
				}
				data, err := io.ReadAll(rawReader)
				rawReader.Close()
				if err != nil {
					continue
				}

				zipReader, err := ReadZipCSV(data)
				if err != nil {
					continue
				}

				csvReader := csv.NewReader(zipReader)
				csvReader.LazyQuotes = true
				csvReader.TrimLeadingSpace = true

				header, err := csvReader.Read()
				if err != nil {
					continue
				}
				colIdx := parseCSVHeader(header)
				isrcIdx, hasISRC := colIdx["isrc"]
				artistIdx, hasArtist := colIdx["artists"]

				if hasISRC && hasArtist {
					for {
						row, err := csvReader.Read()
						if err != nil {
							break
						}
						isrc := getCSVField(row, isrcIdx)
						artist := getCSVField(row, artistIdx)
						if isrc != "" && artist != "" {
							if _, exists := artists[isrc]; !exists {
								artists[isrc] = artist
							}
						}
					}
				}
			}
			s.logger.Info().Int("mappings", len(artists)).Msg("artist mappings after Merlin")
		}
	}

	if len(artists) == 0 {
		s.logger.Warn().Msg("no artist mappings found in any source")
		return 0, nil
	}

	// Batch update assets where artist_name IS NULL
	s.logger.Info().Int("total_mappings", len(artists)).Msg("starting artist_name backfill")
	updated := 0
	batch := 0

	for isrc, artist := range artists {
		tag, err := s.pool.Exec(ctx,
			`UPDATE public.assets a
			 SET artist_name = $1, updated_at = NOW()
			 FROM public.asset_identifiers ai
			 WHERE ai.asset_id = a.id
			   AND ai.identifier_type = 'isrc'
			   AND ai.identifier_value = $2
			   AND ai.effective_to IS NULL
			   AND a.artist_name IS NULL`,
			artist, isrc,
		)
		if err != nil {
			continue
		}
		updated += int(tag.RowsAffected())
		batch++
		if batch%5000 == 0 {
			s.logger.Info().Int("processed", batch).Int("updated", updated).Msg("backfill progress")
		}
	}

	s.logger.Info().Int("processed", batch).Int("updated", updated).Msg("artist_name backfill complete")
	return updated, nil
}
