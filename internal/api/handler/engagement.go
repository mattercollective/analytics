package handler

import (
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/mattercollective/analytics-engine/internal/api/response"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

type EngagementHandler struct {
	engagementRepo *repository.EngagementRepo
}

func NewEngagementHandler(engagementRepo *repository.EngagementRepo) *EngagementHandler {
	return &EngagementHandler{engagementRepo: engagementRepo}
}

// Sources handles GET /api/v1/engagement/sources
func (h *EngagementHandler) Sources(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

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

	sq := repository.SourceQuery{
		StartDate: startDate,
		EndDate:   endDate,
	}

	if assetIDStr := q.Get("asset_id"); assetIDStr != "" {
		id, err := uuid.Parse(assetIDStr)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "invalid asset_id")
			return
		}
		sq.AssetID = &id
	}

	if clientIDStr := q.Get("client_id"); clientIDStr != "" {
		id, err := uuid.Parse(clientIDStr)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "invalid client_id")
			return
		}
		sq.ClientID = &id
	}

	if sq.AssetID == nil && sq.ClientID == nil {
		response.Error(w, http.StatusBadRequest, "either asset_id or client_id is required")
		return
	}

	if platforms := q.Get("platform"); platforms != "" {
		sq.Platforms = strings.Split(platforms, ",")
	}

	results, err := h.engagementRepo.QuerySourceBreakdown(r.Context(), sq)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}

// Rates handles GET /api/v1/engagement/rates
func (h *EngagementHandler) Rates(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	assetIDStr := q.Get("asset_id")
	if assetIDStr == "" {
		response.Error(w, http.StatusBadRequest, "asset_id is required")
		return
	}
	assetID, err := uuid.Parse(assetIDStr)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "invalid asset_id")
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

	result, err := h.engagementRepo.QueryEngagementRates(r.Context(), assetID, startDate, endDate)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, result)
}

// Discovery handles GET /api/v1/engagement/discovery
func (h *EngagementHandler) Discovery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

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

	var assetID *uuid.UUID
	if a := q.Get("asset_id"); a != "" {
		id, err := uuid.Parse(a)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "invalid asset_id")
			return
		}
		assetID = &id
	}

	var clientID *uuid.UUID
	if c := q.Get("client_id"); c != "" {
		id, err := uuid.Parse(c)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "invalid client_id")
			return
		}
		clientID = &id
	}

	if assetID == nil && clientID == nil {
		response.Error(w, http.StatusBadRequest, "either asset_id or client_id is required")
		return
	}

	results, err := h.engagementRepo.QueryDiscoveryTrend(r.Context(), assetID, clientID, startDate, endDate)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}

// Demographics handles GET /api/v1/engagement/demographics
func (h *EngagementHandler) Demographics(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

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

	var assetID *uuid.UUID
	if a := q.Get("asset_id"); a != "" {
		id, err := uuid.Parse(a)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "invalid asset_id")
			return
		}
		assetID = &id
	}

	var clientID *uuid.UUID
	if c := q.Get("client_id"); c != "" {
		id, err := uuid.Parse(c)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "invalid client_id")
			return
		}
		clientID = &id
	}

	if assetID == nil && clientID == nil {
		response.Error(w, http.StatusBadRequest, "either asset_id or client_id is required")
		return
	}

	var platform *string
	if p := q.Get("platform"); p != "" {
		platform = &p
	}

	results, err := h.engagementRepo.QueryDemographics(r.Context(), assetID, clientID, platform, startDate, endDate)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}
