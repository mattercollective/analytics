package platform

import (
	"context"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// RateLimitedTransport wraps an http.RoundTripper with token-bucket rate
// limiting and automatic retry on 429 responses.
type RateLimitedTransport struct {
	Base       http.RoundTripper
	Limiter    *rate.Limiter
	MaxRetries int
}

func NewRateLimitedTransport(base http.RoundTripper, limiter *rate.Limiter) *RateLimitedTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &RateLimitedTransport{
		Base:       base,
		Limiter:    limiter,
		MaxRetries: 5,
	}
}

func (t *RateLimitedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := t.Limiter.Wait(req.Context()); err != nil {
		return nil, err
	}

	var resp *http.Response
	var err error

	for attempt := range t.MaxRetries {
		resp, err = t.Base.RoundTrip(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusTooManyRequests {
			return resp, nil
		}

		resp.Body.Close()

		wait := backoffDuration(attempt, resp)
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(wait):
		}
	}

	return resp, err
}

func backoffDuration(attempt int, resp *http.Response) time.Duration {
	if resp != nil {
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if secs, err := strconv.Atoi(retryAfter); err == nil {
				return time.Duration(secs) * time.Second
			}
		}
	}

	base := math.Pow(2, float64(attempt))
	jitter := rand.Float64() * base * 0.5
	d := time.Duration(base+jitter) * time.Second
	if d > 60*time.Second {
		d = 60 * time.Second
	}
	return d
}

// QuotaLimiter tracks daily quota usage (e.g., YouTube's 10K units/day).
type QuotaLimiter struct {
	DailyLimit int
	used       int
	resetAt    time.Time
}

func NewQuotaLimiter(dailyLimit int) *QuotaLimiter {
	return &QuotaLimiter{
		DailyLimit: dailyLimit,
		resetAt:    nextMidnightUTC(),
	}
}

func (q *QuotaLimiter) Use(ctx context.Context, cost int) error {
	now := time.Now().UTC()
	if now.After(q.resetAt) {
		q.used = 0
		q.resetAt = nextMidnightUTC()
	}

	threshold := int(float64(q.DailyLimit) * 0.9) // reserve 10% for retries
	if q.used+cost > threshold {
		return context.DeadlineExceeded
	}
	q.used += cost
	return nil
}

func (q *QuotaLimiter) Remaining() int {
	now := time.Now().UTC()
	if now.After(q.resetAt) {
		return q.DailyLimit
	}
	return q.DailyLimit - q.used
}

func nextMidnightUTC() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
}
