package handler

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/mattercollective/analytics-engine/internal/api/response"
	"github.com/mattercollective/analytics-engine/internal/model"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

type ArtistsHandler struct {
	metricsRepo *repository.MetricsRepo
}

func NewArtistsHandler(metricsRepo *repository.MetricsRepo) *ArtistsHandler {
	return &ArtistsHandler{metricsRepo: metricsRepo}
}

// ByArtist handles GET /api/v1/analytics/by-artist
func (h *ArtistsHandler) ByArtist(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	clientIDStr := q.Get("client_id")
	if clientIDStr == "" {
		response.Error(w, http.StatusBadRequest, "client_id is required")
		return
	}
	clientID, err := uuid.Parse(clientIDStr)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "invalid client_id")
		return
	}

	startDate, err := time.Parse("2006-01-02", q.Get("start_date"))
	if err != nil {
		response.Error(w, http.StatusBadRequest, "start_date is required (YYYY-MM-DD)")
		return
	}
	endDate, err := time.Parse("2006-01-02", q.Get("end_date"))
	if err != nil {
		response.Error(w, http.StatusBadRequest, "end_date is required (YYYY-MM-DD)")
		return
	}

	limit := 50
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}

	aq := repository.ArtistQuery{
		ClientID:  clientID,
		StartDate: startDate,
		EndDate:   endDate,
		Limit:     limit,
	}

	if platforms := q.Get("platform"); platforms != "" {
		aq.Platforms = strings.Split(platforms, ",")
	}
	if metrics := q.Get("metric"); metrics != "" {
		for _, m := range strings.Split(metrics, ",") {
			aq.Metrics = append(aq.Metrics, model.MetricType(m))
		}
	}

	results, err := h.metricsRepo.QueryByArtist(r.Context(), aq)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}

// TopArtists handles GET /api/v1/analytics/top-artists
func (h *ArtistsHandler) TopArtists(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	clientIDStr := q.Get("client_id")
	if clientIDStr == "" {
		response.Error(w, http.StatusBadRequest, "client_id is required")
		return
	}
	clientID, err := uuid.Parse(clientIDStr)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "invalid client_id")
		return
	}

	metricStr := q.Get("metric")
	if metricStr == "" {
		metricStr = "streams"
	}

	startDate, err := time.Parse("2006-01-02", q.Get("start_date"))
	if err != nil {
		response.Error(w, http.StatusBadRequest, "start_date is required (YYYY-MM-DD)")
		return
	}
	endDate, err := time.Parse("2006-01-02", q.Get("end_date"))
	if err != nil {
		response.Error(w, http.StatusBadRequest, "end_date is required (YYYY-MM-DD)")
		return
	}

	limit := 20
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}

	var platformID *string
	if p := q.Get("platform"); p != "" {
		platformID = &p
	}

	results, err := h.metricsRepo.QueryTopArtists(r.Context(), clientID, model.MetricType(metricStr), platformID, startDate, endDate, limit)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}
