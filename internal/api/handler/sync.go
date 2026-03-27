package handler

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/mattercollective/analytics-engine/internal/api/response"
	"github.com/mattercollective/analytics-engine/internal/ingestion"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

type SyncHandler struct {
	syncRepo     *repository.SyncRepo
	orchestrator *ingestion.Orchestrator
}

func NewSyncHandler(syncRepo *repository.SyncRepo, orchestrator *ingestion.Orchestrator) *SyncHandler {
	return &SyncHandler{
		syncRepo:     syncRepo,
		orchestrator: orchestrator,
	}
}

// Status handles GET /api/v1/sync/status
func (h *SyncHandler) Status(w http.ResponseWriter, r *http.Request) {
	states, err := h.syncRepo.GetAllSyncStates(r.Context())
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}
	response.JSON(w, http.StatusOK, states)
}

// Trigger handles POST /api/v1/sync/trigger/{platform}
// Also handles POST /internal/sync/{platform} for Cloud Scheduler triggers.
func (h *SyncHandler) Trigger(w http.ResponseWriter, r *http.Request) {
	platformID := chi.URLParam(r, "platform")
	if platformID == "" {
		response.Error(w, http.StatusBadRequest, "platform is required")
		return
	}

	// Run sync in background so we can return immediately
	go func() {
		h.orchestrator.SyncPlatform(r.Context(), platformID)
	}()

	response.JSON(w, http.StatusAccepted, map[string]string{
		"message":  "sync triggered",
		"platform": platformID,
	})
}

// Runs handles GET /api/v1/sync/runs
func (h *SyncHandler) Runs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

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

	runs, err := h.syncRepo.ListSyncRuns(r.Context(), platformID, limit)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, runs)
}
