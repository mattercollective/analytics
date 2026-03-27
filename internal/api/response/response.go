package response

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Envelope is the standard API response wrapper.
type Envelope struct {
	Data       any         `json:"data"`
	Meta       Meta        `json:"meta"`
	Pagination *Pagination `json:"pagination,omitempty"`
}

type Meta struct {
	RequestID string `json:"request_id"`
	Timestamp string `json:"timestamp"`
}

type Pagination struct {
	Page       int `json:"page"`
	PerPage    int `json:"per_page"`
	Total      int `json:"total"`
	TotalPages int `json:"total_pages"`
}

// JSON writes a successful JSON response with the standard envelope.
func JSON(w http.ResponseWriter, status int, data any) {
	env := Envelope{
		Data: data,
		Meta: Meta{
			RequestID: uuid.New().String(),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		},
	}
	writeJSON(w, status, env)
}

// JSONWithPagination writes a paginated JSON response.
func JSONWithPagination(w http.ResponseWriter, status int, data any, page, perPage, total int) {
	totalPages := total / perPage
	if total%perPage != 0 {
		totalPages++
	}

	env := Envelope{
		Data: data,
		Meta: Meta{
			RequestID: uuid.New().String(),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		},
		Pagination: &Pagination{
			Page:       page,
			PerPage:    perPage,
			Total:      total,
			TotalPages: totalPages,
		},
	}
	writeJSON(w, status, env)
}

// Error writes an error response.
func Error(w http.ResponseWriter, status int, message string) {
	env := Envelope{
		Data: map[string]string{"error": message},
		Meta: Meta{
			RequestID: uuid.New().String(),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		},
	}
	writeJSON(w, status, env)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
