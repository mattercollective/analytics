package apple

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// NewHTTPClient returns an *http.Client that attaches an Apple Music
// Developer Token (JWT) to every request.
func NewHTTPClient(teamID, keyID, privateKeyPEM string) (*http.Client, error) {
	key, err := parseECPrivateKey(privateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	return &http.Client{
		Transport: &appleTransport{
			base:   http.DefaultTransport,
			teamID: teamID,
			keyID:  keyID,
			key:    key,
		},
	}, nil
}

type appleTransport struct {
	base   http.RoundTripper
	teamID string
	keyID  string
	key    *ecdsa.PrivateKey

	cachedToken string
	expiresAt   time.Time
}

func (t *appleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.getToken()
	if err != nil {
		return nil, err
	}

	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+token)

	return t.base.RoundTrip(req)
}

func (t *appleTransport) getToken() (string, error) {
	if t.cachedToken != "" && time.Now().Before(t.expiresAt.Add(-5*time.Minute)) {
		return t.cachedToken, nil
	}

	now := time.Now()
	exp := now.Add(15 * time.Minute) // Apple Music Analytics tokens valid 20 min, refresh at 15

	claims := jwt.MapClaims{
		"iss": t.teamID, // This is the Issuer ID from iTunes Connect
		"iat": now.Unix(),
		"exp": exp.Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = t.keyID

	signed, err := token.SignedString(t.key)
	if err != nil {
		return "", fmt.Errorf("sign JWT: %w", err)
	}

	t.cachedToken = signed
	t.expiresAt = exp
	return signed, nil
}

func parseECPrivateKey(pemStr string) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	ecKey, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is not ECDSA")
	}

	return ecKey, nil
}
