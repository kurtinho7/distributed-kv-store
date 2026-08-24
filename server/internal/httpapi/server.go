package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"kvstore/internal/apply"
	"kvstore/internal/cluster"
	"kvstore/internal/faults"
	"kvstore/internal/oplog"
	raftstate "kvstore/internal/raft"
	"kvstore/internal/store"
)

const forwardedWriteHeader = "X-KV-Forwarded"

type Server struct {
	store   *store.Memory
	cluster *cluster.State
	faults  *faults.State
	log     *oplog.Log
	raft    *raftstate.State
	writeMu sync.Mutex
	applier *apply.Applier
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

type replicateRequest struct {
	Entry        oplog.Entry `json:"entry"`
	LeaderCommit uint64      `json:"leaderCommit"`
}

func NewServer(store *store.Memory, cluster *cluster.State, log *oplog.Log, raft *raftstate.State, faults *faults.State, applier *apply.Applier) *Server {
	return &Server{store: store, cluster: cluster, log: log, raft: raft, faults: faults, applier: applier}
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
		if r.Header.Get(forwardedWriteHeader) == "true" {
			writeError(w, http.StatusServiceUnavailable, "node is not leader")
			return
		}

		response, err := s.forwardToLeader(r, http.MethodPut, r.URL.Path, req)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "failed to forward request to leader")
			return
		}
		defer response.Body.Close()

		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
		return
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	entry, err := s.log.Append(oplog.OperationPut, key, req.Value)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to append log entry")
		return
	}

	acks := s.replicateToPeers(r, entry)
	if acks < s.cluster.Majority() {
		if err := s.rollbackEntry(entry); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to roll back uncommitted entry")
			return
		}

		writeError(w, http.StatusServiceUnavailable, "failed to reach replication majority")
		return
	}

	if err := s.applier.AdvanceCommit(entry.Index); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to commit log entry")
		return
	}
	s.replicateCommitToPeers(r)
	writeJSON(w, http.StatusOK, keyResponse{Key: key, Value: req.Value})
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	key := keyFromPath(r)

	if s.raft.Role() != raftstate.RoleLeader {
		if r.Header.Get(forwardedWriteHeader) == "true" {
			writeError(w, http.StatusServiceUnavailable, "node is not leader")
			return
		}

		response, err := s.forwardToLeader(r, http.MethodDelete, r.URL.Path, nil)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "failed to forward request to leader")
			return
		}
		defer response.Body.Close()

		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
		return
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

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
		if err := s.rollbackEntry(entry); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to roll back uncommitted entry")
			return
		}

		writeError(w, http.StatusServiceUnavailable, "failed to reach replication majority")
		return
	}
	if err := s.applier.AdvanceCommit(entry.Index); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to commit log entry")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) replicate(w http.ResponseWriter, r *http.Request) {
	var req replicateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if s.raft.Role() == raftstate.RoleLeader {
		writeError(w, http.StatusBadRequest, "leader cannot accept replicated entries")
		return
	}

	if req.Entry.Index == 0 {
		if req.LeaderCommit > 0 {
			if err := s.applier.AdvanceCommit(req.LeaderCommit); err != nil {
				writeError(w, http.StatusInternalServerError, "failed to advance commit index")
				return
			}
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"acknowledged": true,
			"index":        s.log.LastIndex(),
		})
		return
	}

	if err := s.log.AppendEntry(req.Entry); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if req.LeaderCommit > 0 {
		if err := s.applier.AdvanceCommit(req.LeaderCommit); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to advance commit index")
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"acknowledged": true,
		"index":        req.Entry.Index,
	})
}

func (s *Server) replicateToPeers(r *http.Request, entry oplog.Entry) int {
	acks := 1

	for _, peer := range s.cluster.Peers() {
		if s.faults.ShouldDropReplicationTo(peer.ID) {
			continue
		}

		body, err := json.Marshal(replicateRequest{
			Entry:        entry,
			LeaderCommit: s.applier.CommitIndex(),
		})

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

func (s *Server) rollbackEntry(entry oplog.Entry) error {
	if err := s.log.TruncateFrom(entry.Index); err != nil {
		return err
	}

	return s.applier.RebuildCommitted()
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
	var lastErr error

	var encoded []byte
	if payload != nil {
		var err error
		encoded, err = json.Marshal(payload)
		if err != nil {
			return nil, err
		}
	}

	for _, leader := range s.leaderCandidates() {
		var body *bytes.Reader
		if payload == nil {
			body = bytes.NewReader(nil)
		} else {
			body = bytes.NewReader(encoded)
		}

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		request, err := http.NewRequestWithContext(ctx, method, leader.Address+path, body)
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(forwardedWriteHeader, "true")

		response, err := http.DefaultClient.Do(request)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}

		if response.StatusCode == http.StatusInternalServerError || response.StatusCode == http.StatusServiceUnavailable {
			lastErr = fmt.Errorf("candidate %s returned status %d", leader.ID, response.StatusCode)
			_ = response.Body.Close()
			continue
		}

		return response, nil
	}

	if lastErr == nil {
		lastErr = errors.New("leader not known")
	}
	return nil, lastErr
}

func (s *Server) internalLogEntries(w http.ResponseWriter, r *http.Request) {
	if s.raft.Role() != raftstate.RoleLeader {
		writeError(w, http.StatusForbidden, "only leader can serve log entries")
		return
	}

	requestingNodeID := r.Header.Get("X-Node-ID")
	if requestingNodeID != "" && s.faults.ShouldDropReplicationTo(requestingNodeID) {
		writeError(w, http.StatusServiceUnavailable, "node is partitioned from leader")
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

	writeJSON(w, http.StatusOK, map[string]any{
		"entries":     s.log.EntriesAfter(after),
		"lastIndex":   s.log.LastIndex(),
		"commitIndex": s.applier.CommitIndex(),
	})
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

func (s *Server) replicateCommitToPeers(r *http.Request) {
	commitIndex := s.applier.CommitIndex()

	for _, peer := range s.cluster.Peers() {
		if s.faults.ShouldDropReplicationTo(peer.ID) {
			continue
		}

		body, err := json.Marshal(replicateRequest{
			LeaderCommit: commitIndex,
		})
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
		_ = response.Body.Close()
	}
}

func (s *Server) leaderCandidates() []cluster.Member {
	seen := make(map[string]bool)
	candidates := make([]cluster.Member, 0)

	add := func(member cluster.Member) {
		if member.ID == "" || seen[member.ID] {
			return
		}
		seen[member.ID] = true
		candidates = append(candidates, member)
	}

	if leaderID := s.raft.LeaderID(); leaderID != "" {
		if leader, ok := s.cluster.MemberByID(leaderID); ok {
			add(leader)
		}
	}

	for _, peer := range s.cluster.Peers() {
		add(peer)
	}

	return candidates
}
