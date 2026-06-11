package handler

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
)

type HTTPHandler struct {
	log zerolog.Logger
}

func NewHTTPHandler(log zerolog.Logger) *HTTPHandler {
	return &HTTPHandler{log: log.With().Str("component", "http").Logger()}
}

func (h *HTTPHandler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/health", h.health).Methods(http.MethodGet)
}

func (h *HTTPHandler) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}