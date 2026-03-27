package middleware

import (
	"net/http"
	"slices"

	"github.com/mattercollective/analytics-engine/internal/api/response"
)

// APIKeyAuth validates the X-API-Key header against a list of valid keys.
func APIKeyAuth(validKeys []string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("X-API-Key")
			if key == "" {
				response.Error(w, http.StatusUnauthorized, "missing X-API-Key header")
				return
			}

			if !slices.Contains(validKeys, key) {
				response.Error(w, http.StatusForbidden, "invalid API key")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
