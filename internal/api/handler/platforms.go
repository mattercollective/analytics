package handler

import (
	"net/http"

	"github.com/mattercollective/analytics-engine/internal/api/response"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

type PlatformsHandler struct {
	assetRepo *repository.AssetRepo
}

func NewPlatformsHandler(assetRepo *repository.AssetRepo) *PlatformsHandler {
	return &PlatformsHandler{assetRepo: assetRepo}
}

// List handles GET /api/v1/platforms
func (h *PlatformsHandler) List(w http.ResponseWriter, r *http.Request) {
	platforms, err := h.assetRepo.ListPlatforms(r.Context())
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}
	response.JSON(w, http.StatusOK, platforms)
}
