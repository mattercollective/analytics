package spotify

import (
	"context"
	"net/http"

	"golang.org/x/oauth2/clientcredentials"
)

const tokenURL = "https://accounts.spotify.com/api/token"

// NewHTTPClient returns an *http.Client that automatically handles
// OAuth 2.0 Client Credentials token refresh for the Spotify API.
func NewHTTPClient(ctx context.Context, clientID, clientSecret string) *http.Client {
	cfg := &clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     tokenURL,
	}
	return cfg.Client(ctx)
}
