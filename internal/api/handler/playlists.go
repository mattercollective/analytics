package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/mattercollective/analytics-engine/internal/api/response"
	"github.com/mattercollective/analytics-engine/internal/model"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

type PlaylistsHandler struct {
	playlistRepo *repository.PlaylistRepo
}

func NewPlaylistsHandler(playlistRepo *repository.PlaylistRepo) *PlaylistsHandler {
	return &PlaylistsHandler{playlistRepo: playlistRepo}
}

// ForAsset handles GET /api/v1/playlists/for-asset
func (h *PlaylistsHandler) ForAsset(w http.ResponseWriter, r *http.Request) {
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

	date := time.Now().UTC().Truncate(24 * time.Hour)
	if d := q.Get("date"); d != "" {
		parsed, err := time.Parse("2006-01-02", d)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "invalid date (YYYY-MM-DD)")
			return
		}
		date = parsed
	}

	var platform *string
	if p := q.Get("platform"); p != "" {
		platform = &p
	}

	results, err := h.playlistRepo.GetPlaylistsForAsset(r.Context(), assetID, platform, date)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}

// History handles GET /api/v1/playlists/{id}/history
func (h *PlaylistsHandler) History(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	playlistIDStr := chi.URLParam(r, "id")
	playlistID, err := uuid.Parse(playlistIDStr)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "invalid playlist id")
		return
	}

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

	results, err := h.playlistRepo.GetPositionHistory(r.Context(), playlistID, assetID, startDate, endDate)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}

// Top handles GET /api/v1/playlists/top
func (h *PlaylistsHandler) Top(w http.ResponseWriter, r *http.Request) {
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

	limit := 20
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}

	results, err := h.playlistRepo.GetTopPlaylistsForClient(r.Context(), clientID, startDate, endDate, limit)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}

// trackRequest is the body for POST /api/v1/playlists/track
type trackRequest struct {
	PlatformID string  `json:"platform_id"`
	ExternalID string  `json:"external_id"`
	Name       string  `json:"name"`
	ClientID   *string `json:"client_id,omitempty"`
}

// Track handles POST /api/v1/playlists/track
func (h *PlaylistsHandler) Track(w http.ResponseWriter, r *http.Request) {
	var req trackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.PlatformID == "" || req.ExternalID == "" || req.Name == "" {
		response.Error(w, http.StatusBadRequest, "platform_id, external_id, and name are required")
		return
	}

	playlistID, err := h.playlistRepo.UpsertPlaylist(r.Context(), model.PlaylistUpsert{
		PlatformID: req.PlatformID,
		ExternalID: req.ExternalID,
		Name:       req.Name,
	})
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "failed to create playlist")
		return
	}

	var clientID *uuid.UUID
	if req.ClientID != nil {
		id, err := uuid.Parse(*req.ClientID)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "invalid client_id")
			return
		}
		clientID = &id
	}

	if err := h.playlistRepo.AddTrackedPlaylist(r.Context(), playlistID, clientID, "api"); err != nil {
		response.Error(w, http.StatusInternalServerError, "failed to track playlist")
		return
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"playlist_id": playlistID,
		"tracked":     true,
	})
}

// Tracked handles GET /api/v1/playlists/tracked
func (h *PlaylistsHandler) Tracked(w http.ResponseWriter, r *http.Request) {
	var clientID *uuid.UUID
	if c := r.URL.Query().Get("client_id"); c != "" {
		id, err := uuid.Parse(c)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "invalid client_id")
			return
		}
		clientID = &id
	}

	results, err := h.playlistRepo.ListTrackedPlaylists(r.Context(), clientID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSON(w, http.StatusOK, results)
}
