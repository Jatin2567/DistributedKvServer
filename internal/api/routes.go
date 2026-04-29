package api

import "net/http"

func RegisterRoutes(mux *http.ServeMux, h *Handler) {
	mux.HandleFunc("/put", h.HandlePut)
	mux.HandleFunc("/get", h.HandleGet)
	mux.HandleFunc("/delete", h.HandleDelete)
}