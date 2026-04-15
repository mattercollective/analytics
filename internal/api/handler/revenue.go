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

type RevenueHandler struct {
	revenueRepo *repository.RevenueRepo
}

func NewRevenueHandler(revenueRepo *repository.RevenueRepo) *RevenueHandler {
	return &RevenueHandler{revenueRepo: revenueRepo}
}

// Summary handles GET /api/v1/revenue/summary
func (h *RevenueHandler) Summary(w http.ResponseWriter, r *http.Request) {
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

	sq := repository.RevenueSummaryQuery{
		StartDate:   startDate,
		EndDate:     endDate,
		Granularity: q.Get("granularity"),
	}
	if sq.Granularity == "" {
		sq.Granularity = "daily"
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

	rows, err := h.revenueRepo.QuerySummary(r.Context(), sq)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	summary := buildRevenueSummary(sq, rows)
	response.JSON(w, http.StatusOK, summary)
}

func buildRevenueSummary(sq repository.RevenueSummaryQuery, rows []repository.RevenueSummaryDBRow) model.RevenueSummary {
	summary := model.RevenueSummary{
		AssetID:     sq.AssetID,
		ClientID:    sq.ClientID,
		Period: model.DateRange{
			Start: sq.StartDate.Format("2006-01-02"),
			End:   sq.EndDate.Format("2006-01-02"),
		},
		Granularity: sq.Granularity,
	}

	dateMap := make(map[string]*model.RevenueSummaryRow)
	for _, row := range rows {
		dateStr := row.PeriodDate.Format("2006-01-02")

		point, ok := dateMap[dateStr]
		if !ok {
			point = &model.RevenueSummaryRow{
				Date:       dateStr,
				ByPlatform: make(map[string]float64),
			}
			dateMap[dateStr] = point
		}

		point.TotalUSD += row.TotalUSD
		point.ByPlatform[row.PlatformID] += row.TotalUSD
		summary.TotalUSD += row.TotalUSD
	}

	for _, point := range dateMap {
		summary.Series = append(summary.Series, *point)
	}

	return summary
}

// BySource handles GET /api/v1/revenue/by-source
func (h *RevenueHandler) BySource(w http.ResponseWriter, r *http.Request) {
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

	bsq := repository.RevenueBySourceQuery{
		StartDate: startDate,
		EndDate:   endDate,
	}

	if assetIDStr := q.Get("asset_id"); assetIDStr != "" {
		id, err := uuid.Parse(assetIDStr)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "invalid asset_id")
			return
		}
		bsq.AssetID = &id
	}

	if clientIDStr := q.Get("client_id"); clientIDStr != "" {
		id, err := uuid.Parse(clientIDStr)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "invalid client_id")
			return
		}
		bsq.ClientID = &id
	}

	if bsq.AssetID == nil && bsq.ClientID == nil {
		response.Error(w, http.StatusBadRequest, "either asset_id or client_id is required")
		return
	}

	if platforms := q.Get("platform"); platforms != "" {
		bsq.Platforms = strings.Split(platforms, ",")
	}

	rows, err := h.revenueRepo.QueryBySource(r.Context(), bsq)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	result := buildRevenueBySource(startDate, endDate, rows)
	response.JSON(w, http.StatusOK, result)
}

func buildRevenueBySource(startDate, endDate time.Time, rows []repository.RevenueBySourceDBRow) model.RevenueBySource {
	result := model.RevenueBySource{
		Period: model.DateRange{
			Start: startDate.Format("2006-01-02"),
			End:   endDate.Format("2006-01-02"),
		},
	}

	sourceMap := make(map[string]*model.RevenueSourceBreakdown)
	for _, row := range rows {
		sb, ok := sourceMap[row.Source]
		if !ok {
			sb = &model.RevenueSourceBreakdown{
				Source: row.Source,
				ByType: make(map[string]float64),
			}
			sourceMap[row.Source] = sb
		}
		sb.TotalUSD += row.TotalUSD
		sb.ByType[row.Type] += row.TotalUSD
	}

	for _, sb := range sourceMap {
		result.Sources = append(result.Sources, *sb)
	}

	return result
}

// ByTerritory handles GET /api/v1/revenue/by-territory
func (h *RevenueHandler) ByTerritory(w http.ResponseWriter, r *http.Request) {
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

	limit := 50
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}

	btq := repository.RevenueByTerritoryQuery{
		AssetID:   assetID,
		StartDate: startDate,
		EndDate:   endDate,
		Limit:     limit,
	}

	if platforms := q.Get("platform"); platforms != "" {
		btq.Platforms = strings.Split(platforms, ",")
	}

	results, err := h.revenueRepo.QueryByTerritory(r.Context(), btq)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}

// ByPlatform handles GET /api/v1/revenue/by-platform
func (h *RevenueHandler) ByPlatform(w http.ResponseWriter, r *http.Request) {
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

	results, err := h.revenueRepo.QueryByPlatform(r.Context(), repository.RevenueByPlatformQuery{
		ClientID:  clientID,
		StartDate: startDate,
		EndDate:   endDate,
	})
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}
