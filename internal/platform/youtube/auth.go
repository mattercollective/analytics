package youtube

import (
	"context"
	"fmt"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const (
	scopeYouTubeAnalytics = "https://www.googleapis.com/auth/yt-analytics.readonly"
	scopeYouTubePartner   = "https://www.googleapis.com/auth/youtubepartner"
)

// NewAuthenticatedClient creates an *http.Client authenticated via a GCP service account.
func NewAuthenticatedClient(ctx context.Context, serviceAccountJSON string) (*http.Client, error) {
	creds, err := google.CredentialsFromJSON(ctx, []byte(serviceAccountJSON),
		scopeYouTubeAnalytics,
		scopeYouTubePartner,
	)
	if err != nil {
		return nil, fmt.Errorf("parse service account: %w", err)
	}

	return oauth2.NewClient(ctx, creds.TokenSource), nil
}
