package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	// Server
	Port string
	Env  string // "development", "staging", "production"

	// Database
	DatabaseURL     string
	DatabasePoolMax int
	DatabasePoolMin int

	// API Auth
	APIKeys []string

	// CORS
	CORSOrigins []string

	// Spotify
	SpotifyClientID     string
	SpotifyClientSecret string
	SpotifyLicensorID   string

	// Apple Music
	AppleTeamID    string
	AppleKeyID     string
	ApplePrivateKey string

	// YouTube
	YouTubeServiceAccountJSON string
	YouTubeContentOwnerID     string

	// Amazon
	AmazonAPIKey    string
	AmazonAPISecret string

	// GCP
	GCPProjectID string
	PubSubTopic  string

	// Logging
	LogLevel  string
	LogFormat string // "json" or "text"
}

func Load() (*Config, error) {
	cfg := &Config{
		Port:            envOrDefault("PORT", "8080"),
		Env:             envOrDefault("ENV", "development"),
		DatabaseURL:     os.Getenv("DATABASE_URL"),
		DatabasePoolMax: envIntOrDefault("DATABASE_POOL_MAX", 20),
		DatabasePoolMin: envIntOrDefault("DATABASE_POOL_MIN", 2),
		APIKeys:         splitCSV(os.Getenv("API_KEYS")),
		CORSOrigins:     splitCSV(os.Getenv("CORS_ORIGINS")),

		SpotifyClientID:     os.Getenv("SPOTIFY_CLIENT_ID"),
		SpotifyClientSecret: os.Getenv("SPOTIFY_CLIENT_SECRET"),
		SpotifyLicensorID:   os.Getenv("SPOTIFY_LICENSOR_ID"),

		AppleTeamID:    os.Getenv("APPLE_TEAM_ID"),
		AppleKeyID:     os.Getenv("APPLE_KEY_ID"),
		ApplePrivateKey: os.Getenv("APPLE_PRIVATE_KEY"),

		YouTubeServiceAccountJSON: os.Getenv("YOUTUBE_SERVICE_ACCOUNT_JSON"),
		YouTubeContentOwnerID:     os.Getenv("YOUTUBE_CONTENT_OWNER_ID"),

		AmazonAPIKey:    os.Getenv("AMAZON_API_KEY"),
		AmazonAPISecret: os.Getenv("AMAZON_API_SECRET"),

		GCPProjectID: os.Getenv("GCP_PROJECT_ID"),
		PubSubTopic:  envOrDefault("PUBSUB_TOPIC", "analytics-sync-trigger"),

		LogLevel:  envOrDefault("LOG_LEVEL", "info"),
		LogFormat: envOrDefault("LOG_FORMAT", "json"),
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
