package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mattercollective/analytics-engine/internal/api/response"
)

type HealthHandler struct {
	pool *pgxpool.Pool
}

func NewHealthHandler(pool *pgxpool.Pool) *HealthHandler {
	return &HealthHandler{pool: pool}
}

func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	dbOK := "ok"
	if err := h.pool.Ping(ctx); err != nil {
		dbOK = "error: " + err.Error()
	}

	response.JSON(w, http.StatusOK, map[string]string{
		"status":   "ok",
		"database": dbOK,
	})
}
