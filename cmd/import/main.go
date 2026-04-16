package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/mattercollective/analytics-engine/internal/config"
	"github.com/mattercollective/analytics-engine/internal/database"
	"github.com/mattercollective/analytics-engine/internal/importer"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

const defaultBucket = "matter-reports-raw"

func main() {
	// Flags
	platform := flag.String("platform", "", "Platform to import: apple, amazon, merlin, apple-reports (required)")
	bucket := flag.String("bucket", defaultBucket, "GCS bucket name")
	dryRun := flag.Bool("dry-run", false, "List files without importing")
	seedOnly := flag.Bool("seed", false, "Only seed assets (extract ISRCs, create asset records)")
	backfillArtists := flag.Bool("backfill-artists", false, "Backfill artist_name on existing assets from report data")
	cmsSync := flag.Bool("cms-sync", false, "Sync organizations, channels, and YT assets from CMS Supabase")
	cmsURL := flag.String("cms-url", "https://qckfotfuiowzzjoczmau.supabase.co", "CMS Supabase API URL")
	cmsKey := flag.String("cms-key", "", "CMS Supabase service_role key")
	flag.Parse()

	if *backfillArtists {
		// Skip to backfill mode — handled after DB and GCS init
	} else if *cmsSync {
		if *cmsKey == "" {
			fmt.Println("Usage: import -cms-sync -cms-key <service_role_key>")
			os.Exit(1)
		}
	} else if *platform == "" {
		fmt.Println("Usage: import -platform <apple|amazon|merlin|all> [-bucket <name>] [-dry-run] [-seed]")
		fmt.Println("       import -cms-sync -cms-key <service_role_key>")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()

	// Config & DB
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to load config")
	}

	pool, err := database.NewPool(ctx, cfg.DatabaseURL, cfg.DatabasePoolMin, cfg.DatabasePoolMax)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer pool.Close()
	logger.Info().Msg("database connected")

	// GCS
	gcsClient, err := importer.NewGCSClient(ctx)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create GCS client")
	}
	defer gcsClient.Close()
	logger.Info().Str("bucket", *bucket).Msg("GCS client ready")

	// Backfill artist names mode
	if *backfillArtists {
		seeder := importer.NewAssetSeeder(gcsClient, pool, logger, *bucket)
		applePath := "rebel-apple-reports/sales/amcontent/detailed/daily/AppleMusic_Content_93824149_20260316_V1_2.txt"
		updated, err := seeder.BackfillArtistNames(ctx, applePath, "amazon/", "merlin/trends/")
		if err != nil {
			logger.Fatal().Err(err).Msg("artist backfill failed")
		}
		logger.Info().Int("updated", updated).Msg("artist backfill complete")
		return
	}

	// CMS Sync mode
	if *cmsSync {
		sync := importer.NewCMSSync(pool, *cmsURL, *cmsKey, logger)

		orgs, err := sync.SyncOrganizations(ctx)
		if err != nil {
			logger.Fatal().Err(err).Msg("org sync failed")
		}
		logger.Info().Int("orgs", orgs).Msg("organizations synced")

		channels, err := sync.SyncChannels(ctx)
		if err != nil {
			logger.Fatal().Err(err).Msg("channel sync failed")
		}
		logger.Info().Int("channels", channels).Msg("channels synced")

		batchSync := importer.NewCMSSyncBatch(pool, *cmsURL, *cmsKey, logger)
		assets, mappings, err := batchSync.SyncYouTubeAssetsBatch(ctx)
		if err != nil {
			logger.Fatal().Err(err).Msg("YouTube asset sync failed")
		}
		logger.Info().Int("assets", assets).Int("mappings", mappings).Msg("YouTube assets synced")

		return
	}

	metricsRepo := repository.NewMetricsRepo(pool)

	// Seed mode: extract ISRCs and create asset records
	if *seedOnly {
		seeder := importer.NewAssetSeeder(gcsClient, pool, logger, *bucket)
		switch *platform {
		case "apple":
			contentPath := "rebel-apple-reports/sales/amcontent/detailed/daily/AppleMusic_Content_93824149_20260316_V1_2.txt"
			n, err := seeder.SeedFromApple(ctx, contentPath)
			if err != nil {
				logger.Fatal().Err(err).Msg("Apple seed failed")
			}
			logger.Info().Int("created", n).Msg("Apple seeding done")
		case "amazon":
			n, err := seeder.SeedFromAmazon(ctx, "amazon/")
			if err != nil {
				logger.Fatal().Err(err).Msg("Amazon seed failed")
			}
			logger.Info().Int("created", n).Msg("Amazon seeding done")
		case "merlin":
			n, err := seeder.SeedFromMerlin(ctx, "merlin/trends/")
			if err != nil {
				logger.Fatal().Err(err).Msg("Merlin seed failed")
			}
			logger.Info().Int("created", n).Msg("Merlin seeding done")
		case "all":
			contentPath := "rebel-apple-reports/sales/amcontent/detailed/daily/AppleMusic_Content_93824149_20260316_V1_2.txt"
			n1, _ := seeder.SeedFromApple(ctx, contentPath)
			n2, _ := seeder.SeedFromAmazon(ctx, "amazon/")
			n3, _ := seeder.SeedFromMerlin(ctx, "merlin/trends/")
			logger.Info().Int("apple", n1).Int("amazon", n2).Int("merlin", n3).Int("total", n1+n2+n3).Msg("all seeding done")
		}
		return
	}

	playlistRepo := repository.NewPlaylistRepo(pool)
	engagementRepo := repository.NewEngagementRepo(pool)

	switch *platform {
	case "apple":
		runAppleImport(ctx, gcsClient, metricsRepo, logger, *bucket, *dryRun)
	case "apple-reports":
		runAppleReportsImport(ctx, gcsClient, metricsRepo, playlistRepo, engagementRepo, logger, *bucket)
	case "amazon":
		runAmazonImport(ctx, gcsClient, metricsRepo, logger, *bucket, *dryRun)
	case "merlin":
		runMerlinImport(ctx, gcsClient, metricsRepo, logger, *bucket, *dryRun)
	default:
		logger.Fatal().Str("platform", *platform).Msg("unknown platform")
	}
}

func runAppleImport(ctx context.Context, gcs *importer.GCSClient, metricsRepo *repository.MetricsRepo, logger zerolog.Logger, bucket string, dryRun bool) {
	imp := importer.NewAppleImporter(gcs, metricsRepo, logger, bucket)

	// Load Apple Identifier -> ISRC mapping
	contentPath := "rebel-apple-reports/sales/amcontent/detailed/daily/AppleMusic_Content_93824149_20260316_V1_2.txt"
	if err := imp.LoadContentMapping(ctx, contentPath); err != nil {
		logger.Fatal().Err(err).Msg("failed to load Apple content mapping")
	}

	if dryRun {
		files, _ := gcs.ListFiles(ctx, bucket, "rebel-apple-reports/sales/amstreams/daily/")
		fmt.Printf("Would import %d Apple streams files\n", len(files))
		for _, f := range files {
			fmt.Println("  ", f)
		}
		return
	}

	if err := imp.ImportAllStreams(ctx, "rebel-apple-reports/sales/amstreams/daily/"); err != nil {
		logger.Fatal().Err(err).Msg("Apple import failed")
	}
}

func runAmazonImport(ctx context.Context, gcs *importer.GCSClient, metricsRepo *repository.MetricsRepo, logger zerolog.Logger, bucket string, dryRun bool) {
	imp := importer.NewAmazonImporter(gcs, metricsRepo, logger, bucket)

	if dryRun {
		files, _ := gcs.ListFiles(ctx, bucket, "amazon/")
		fmt.Printf("Would import %d Amazon files\n", len(files))
		for _, f := range files {
			fmt.Println("  ", f)
		}
		return
	}

	if err := imp.ImportAll(ctx, "amazon/"); err != nil {
		logger.Fatal().Err(err).Msg("Amazon import failed")
	}
}

func runMerlinImport(ctx context.Context, gcs *importer.GCSClient, metricsRepo *repository.MetricsRepo, logger zerolog.Logger, bucket string, dryRun bool) {
	imp := importer.NewMerlinImporter(gcs, metricsRepo, logger, bucket)

	if dryRun {
		files, _ := gcs.ListFiles(ctx, bucket, "merlin/trends/")
		fmt.Printf("Would import %d Merlin trends files\n", len(files))
		for _, f := range files {
			fmt.Println("  ", f)
		}
		return
	}

	if err := imp.ImportAllTrends(ctx, "merlin/trends/"); err != nil {
		logger.Fatal().Err(err).Msg("Merlin import failed")
	}
}

func runAppleReportsImport(ctx context.Context, gcs *importer.GCSClient, metricsRepo *repository.MetricsRepo, playlistRepo *repository.PlaylistRepo, engagementRepo *repository.EngagementRepo, logger zerolog.Logger, bucket string) {
	imp := importer.NewAppleReportsImporter(gcs, metricsRepo, playlistRepo, engagementRepo, logger, bucket)

	// Load Apple ID → ISRC mapping first
	contentPath := "rebel-apple-reports/sales/amcontent/detailed/daily/AppleMusic_Content_93824149_20260316_V1_2.txt"
	if err := imp.LoadContentMapping(ctx, contentPath); err != nil {
		logger.Fatal().Err(err).Msg("failed to load Apple content mapping")
	}

	// 1. Editorial Playlist Adds
	logger.Info().Msg("importing editorial playlist adds...")
	n1, err := imp.ImportEditorialPlaylistAdds(ctx, "rebel-apple-reports/sales/ameditorialplaylistadds/")
	if err != nil {
		logger.Error().Err(err).Msg("editorial playlist import failed")
	}
	logger.Info().Int("rows", n1).Msg("editorial playlist adds done")

	// 2. Demographics
	logger.Info().Msg("importing demographics...")
	n2, err := imp.ImportDemographics(ctx, "rebel-apple-reports/sales/amcontentdemographics/")
	if err != nil {
		logger.Error().Err(err).Msg("demographics import failed")
	}
	logger.Info().Int("rows", n2).Msg("demographics done")

	// 3. Shazams
	logger.Info().Msg("importing shazams...")
	n3, err := imp.ImportShazams(ctx, "rebel-apple-reports/sales/amshazam/")
	if err != nil {
		logger.Error().Err(err).Msg("shazam import failed")
	}
	logger.Info().Int("rows", n3).Msg("shazams done")

	// 4. Container/Source of Stream
	logger.Info().Msg("importing containers (source of stream)...")
	n4, err := imp.ImportContainers(ctx, "rebel-apple-reports/sales/amcontainer/")
	if err != nil {
		logger.Error().Err(err).Msg("container import failed")
	}
	logger.Info().Int("rows", n4).Msg("containers done")

	logger.Info().
		Int("playlist_adds", n1).
		Int("demographics", n2).
		Int("shazams", n3).
		Int("containers", n4).
		Int("total", n1+n2+n3+n4).
		Msg("all Apple reports imported")
}
