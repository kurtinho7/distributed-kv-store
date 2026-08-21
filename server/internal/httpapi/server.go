package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"kvstore/internal/cluster"
	"kvstore/internal/oplog"
	"kvstore/internal/store"
	raftstate "kvstore/internal/raft"
	"kvstore/internal/faults"
)

type Server struct {
	store   *store.Memory
	cluster *cluster.State
	faults  *faults.State
	log     *oplog.Log
	raft    *raftstate.State
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

func NewServer(store *store.Memory, cluster *cluster.State, log *oplog.Log, raft *raftstate.State, faults *faults.State) *Server {
	return &Server{store: store, cluster: cluster, log: log, raft: raft, faults: faults}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /cluster", s.clusterState)
	mux.HandleFunc("GET /log", s.logEntries)
	mux.HandleFunc("GET /internal/log", s.internalLogEntries)
	mux.HandleFunc("GET /raft", s.raftState)
	mux.HandleFunc("POST /internal/replicate", s.replicate)
	mux.HandleFunc("POST /internal/raft/request-vote", s.requestVote)
	mux.HandleFunc("POST /internal/raft/append-entries", s.appendEntries)
	mux.HandleFunc("GET /kv", s.list)
	mux.HandleFunc("GET /kv/", s.get)
	mux.HandleFunc("PUT /kv/", s.put)
	mux.HandleFunc("DELETE /kv/", s.delete)
	mux.HandleFunc("GET /faults", s.faultState)
	mux.HandleFunc("POST /faults/replication/", s.dropReplication)
	mux.HandleFunc("DELETE /faults/replication/", s.healReplication)
	return withCORS(mux)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) clusterState(w http.ResponseWriter, _ *http.Request) {
	members := s.cluster.Snapshot(s.log.LastIndex())
	leaderID := s.raft.LeaderID()

	for i := range members {
		if leaderID != "" && members[i].ID == leaderID {
			members[i].Role = cluster.RoleLeader
		} else {
			members[i].Role = cluster.RoleFollower
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"members": members})
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

	if s.raft.Role() != raftstate.RoleLeader {
		response, err := s.forwardToLeader(r, http.MethodPut, r.URL.Path, req)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to forward request to leader")
			return
		}
		defer response.Body.Close()

		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
		return
	}

	entry, err := s.log.Append(oplog.OperationPut, key, req.Value)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to append log entry")
		return
	}

	acks := s.replicateToPeers(r, entry)
	if acks < s.cluster.Majority() {
		writeError(w, http.StatusServiceUnavailable, "failed to reach replication majority")
		return
	}

	if err := s.store.Apply(entry); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to apply log entry")
		return
	}
	writeJSON(w, http.StatusOK, keyResponse{Key: key, Value: req.Value})
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	key := keyFromPath(r)

	if s.raft.Role() != raftstate.RoleLeader {
		response, err := s.forwardToLeader(r, http.MethodDelete, r.URL.Path, nil)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to forward request to leader")
			return
		}
		defer response.Body.Close()

		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
		return
	}

	if _, err := s.store.Get(key); errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}

	entry, err := s.log.Append(oplog.OperationDelete, key, "")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to append log entry")
		return
	}
	acks := s.replicateToPeers(r, entry)
	if acks < s.cluster.Majority() {
		writeError(w, http.StatusServiceUnavailable, "failed to reach replication majority")
		return
	}
	if err := s.store.Apply(entry); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to apply log entry")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) replicate(w http.ResponseWriter, r *http.Request) {
	var entry oplog.Entry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if s.cluster.IsLeader() {
		writeError(w, http.StatusBadRequest, "leader cannot accept replicated entries")
		return
	}

	if err := s.log.AppendEntry(entry); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := s.store.Apply(entry); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to apply log entry")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"acknowledged": true,
		"index":        entry.Index,
	})
}

func (s *Server) replicateToPeers(r *http.Request, entry oplog.Entry) int {
	acks := 1

	for _, peer := range s.cluster.Peers() {
		if s.faults.ShouldDropReplicationTo(peer.ID) {
			continue
		}

		body, err := json.Marshal(entry)
		if err != nil {
			continue
		}

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, peer.Address+"/internal/replicate", bytes.NewBuffer(body))
		if err != nil {
			cancel()
			continue
		}

		request.Header.Set("Content-Type", "application/json")

		response, err := http.DefaultClient.Do(request)
		cancel()
		if err != nil {
			continue
		}

		if response.Body != nil {
			_ = response.Body.Close()
		}

		if response.StatusCode >= 200 && response.StatusCode < 300 {
			acks++
		}
	}
	return acks
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

func (s *Server) forwardToLeader(r *http.Request, method, path string, payload any) (*http.Response, error) {
	leaderID := s.raft.LeaderID()
	if leaderID == "" {
		return nil, errors.New("leader not known")
	}

	leader, ok := s.cluster.MemberByID(leaderID)
	if !ok {
		return nil, errors.New("leader not found in cluster")
	}

	var body *bytes.Reader
	if payload == nil {
		body = bytes.NewReader(nil)
	} else {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(encoded)
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, method, leader.Address+path, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")

	return http.DefaultClient.Do(request)
}

func (s *Server) internalLogEntries(w http.ResponseWriter, r *http.Request) {
	if s.raft.Role() != raftstate.RoleLeader {
		writeError(w, http.StatusForbidden, "only leader can serve log entries")
		return
	}

	after := uint64(0)
	rawAfter := r.URL.Query().Get("after")
	if rawAfter != "" {
		parsed, err := strconv.ParseUint(rawAfter, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid 'after' query parameter")
			return
		}
		after = parsed
	}

	writeJSON(w, http.StatusOK, map[string]any{"entries": s.log.EntriesAfter(after)})
}

func (s *Server) requestVote(w http.ResponseWriter, r *http.Request) {
	var req raftstate.RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	resp := s.raft.RequestVote(req)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) appendEntries(w http.ResponseWriter, r *http.Request) {
	var req raftstate.AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	resp := s.raft.AppendEntries(req)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) raftState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.raft.Snapshot())
}

func (s *Server) faultState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"droppedReplicationTo": s.faults.DroppedReplicationTargets(),
	})
}

func (s *Server) dropReplication(w http.ResponseWriter, r *http.Request) {
	nodeID := strings.TrimPrefix(r.URL.Path, "/faults/replication/")
	if nodeID == "" {
		writeError(w, http.StatusBadRequest, "node ID is required")
		return
	}

	s.faults.DropReplicationTo(nodeID)
	writeJSON(w, http.StatusOK, map[string]any{
		"droppedReplicationTo": s.faults.DroppedReplicationTargets(),
	})
}

func (s *Server) healReplication(w http.ResponseWriter, r *http.Request) {
	nodeID := strings.TrimPrefix(r.URL.Path, "/faults/replication/")
	if nodeID == "" {
		writeError(w, http.StatusBadRequest, "node ID is required")
		return
	}

	s.faults.HealReplicationTo(nodeID)
	writeJSON(w, http.StatusOK, map[string]any{
		"droppedReplicationTo": s.faults.DroppedReplicationTargets(),
	})
}
