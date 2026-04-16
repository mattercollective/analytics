package handler

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/mattercollective/analytics-engine/internal/api/response"
	"github.com/mattercollective/analytics-engine/internal/repository"
)

type AssetsHandler struct {
	assetRepo *repository.AssetRepo
}

func NewAssetsHandler(assetRepo *repository.AssetRepo) *AssetsHandler {
	return &AssetsHandler{assetRepo: assetRepo}
}

// List handles GET /api/v1/assets
func (h *AssetsHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	page := 1
	if p := q.Get("page"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			page = n
		}
	}

	perPage := 50
	if pp := q.Get("per_page"); pp != "" {
		if n, err := strconv.Atoi(pp); err == nil && n > 0 && n <= 100 {
			perPage = n
		}
	}

	var search *string
	if s := q.Get("search"); s != "" {
		search = &s
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

	var isrc *string
	if i := q.Get("isrc"); i != "" {
		isrc = &i
	}

	var upc *string
	if u := q.Get("upc"); u != "" {
		upc = &u
	}

	assets, total, err := h.assetRepo.ListAssets(r.Context(), search, clientID, isrc, upc, page, perPage)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}

	response.JSONWithPagination(w, http.StatusOK, assets, page, perPage, total)
}

// Get handles GET /api/v1/assets/{id}
func (h *AssetsHandler) Get(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "invalid asset id")
		return
	}

	asset, err := h.assetRepo.GetAsset(r.Context(), id)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "query failed")
		return
	}
	if asset == nil {
		response.Error(w, http.StatusNotFound, "asset not found")
		return
	}

	response.JSON(w, http.StatusOK, asset)
}
