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

type AnalyticsHandler struct {
	metricsRepo *repository.MetricsRepo
}

func NewAnalyticsHandler(metricsRepo *repository.MetricsRepo) *AnalyticsHandler {
	return &AnalyticsHandler{metricsRepo: metricsRepo}
}

// Summary handles GET /api/v1/analytics/summary
func (h *AnalyticsHandler) Summary(w http.ResponseWriter, r *http.Request) {
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

	sq := repository.SummaryQuery{
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
	if metrics := q.Get("metric"); metrics != "" {
		for _, m := range strings.Split(metrics, ",") {
			sq.Metrics = append(sq.Metrics, model.MetricType(m))
		}
	}
	if territories := q.Get("territory"); territories != "" {
		sq.Territories = strings.Split(territories, ",")
	}

	rows, err := h.metricsRepo.QuerySummary(r.Context(), sq)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	summary := buildSummary(sq, rows)
	response.JSON(w, http.StatusOK, summary)
}

func buildSummary(sq repository.SummaryQuery, rows []repository.SummaryRow) model.AnalyticsSummary {
	summary := model.AnalyticsSummary{
		AssetID:     sq.AssetID,
		ClientID:    sq.ClientID,
		Period: model.DateRange{
			Start: sq.StartDate.Format("2006-01-02"),
			End:   sq.EndDate.Format("2006-01-02"),
		},
		Granularity: sq.Granularity,
		Totals:      make(map[model.MetricType]int64),
	}

	// Group rows by date
	dateMap := make(map[string]*model.AnalyticsSummaryPoint)
	for _, row := range rows {
		dateStr := row.PeriodDate.Format("2006-01-02")

		point, ok := dateMap[dateStr]
		if !ok {
			point = &model.AnalyticsSummaryPoint{
				Date:       dateStr,
				Metrics:    make(map[model.MetricType]int64),
				ByPlatform: make(map[string]map[model.MetricType]int64),
			}
			dateMap[dateStr] = point
		}

		point.Metrics[row.MetricType] += row.TotalValue
		summary.Totals[row.MetricType] += row.TotalValue

		if _, ok := point.ByPlatform[row.PlatformID]; !ok {
			point.ByPlatform[row.PlatformID] = make(map[model.MetricType]int64)
		}
		point.ByPlatform[row.PlatformID][row.MetricType] += row.TotalValue
	}

	// Convert to sorted slice
	for _, point := range dateMap {
		summary.Series = append(summary.Series, *point)
	}

	return summary
}

// TopAssets handles GET /api/v1/analytics/top-assets
func (h *AnalyticsHandler) TopAssets(w http.ResponseWriter, r *http.Request) {
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
		response.Error(w, http.StatusBadRequest, "metric is required")
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

	results, err := h.metricsRepo.QueryTopAssets(r.Context(), clientID, model.MetricType(metricStr), platformID, startDate, endDate, limit)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}

// Territories handles GET /api/v1/analytics/territories
func (h *AnalyticsHandler) Territories(w http.ResponseWriter, r *http.Request) {
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

	limit := 50
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}

	var platformID *string
	if p := q.Get("platform"); p != "" {
		platformID = &p
	}

	results, err := h.metricsRepo.QueryTerritories(r.Context(), assetID, platformID, model.MetricType(metricStr), startDate, endDate, limit)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}
