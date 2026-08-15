package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"kvstore/internal/cluster"
	"kvstore/internal/oplog"
	"kvstore/internal/store"
)

type Server struct {
	store   *store.Memory
	cluster *cluster.State
	log     *oplog.Log
}

type putRequest struct {
	Value string `json:"value"`
}

type keyResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func NewServer(store *store.Memory, cluster *cluster.State, log *oplog.Log) *Server {
	return &Server{store: store, cluster: cluster, log: log}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /cluster", s.clusterState)
	mux.HandleFunc("GET /log", s.logEntries)
	mux.HandleFunc("GET /kv", s.list)
	mux.HandleFunc("GET /kv/", s.get)
	mux.HandleFunc("PUT /kv/", s.put)
	mux.HandleFunc("DELETE /kv/", s.delete)
	return withCORS(mux)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) clusterState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"members": s.cluster.Snapshot(s.log.LastIndex())})
}

func (s *Server) logEntries(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"entries": s.log.Entries()})
}

func (s *Server) list(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"entries": s.store.Snapshot()})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	key := keyFromPath(r)
	value, err := s.store.Get(key)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	writeJSON(w, http.StatusOK, keyResponse{Key: key, Value: value})
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	key := keyFromPath(r)
	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	entry := s.log.Append(oplog.OperationPut, key, req.Value)
	if err := s.store.Apply(entry); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to apply log entry")
		return
	}
	writeJSON(w, http.StatusOK, keyResponse{Key: key, Value: req.Value})
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	key := keyFromPath(r)
	if _, err := s.store.Get(key); errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}

	entry := s.log.Append(oplog.OperationDelete, key, "")
	if err := s.store.Apply(entry); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to apply log entry")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func keyFromPath(r *http.Request) string {
	return strings.TrimPrefix(r.URL.Path, "/kv/")
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
