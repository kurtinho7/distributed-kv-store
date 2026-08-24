package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kvstore/internal/apply"
	"kvstore/internal/cluster"
	"kvstore/internal/faults"
	"kvstore/internal/oplog"
	raftstate "kvstore/internal/raft"
	"kvstore/internal/store"
)

func TestKVLifecycle(t *testing.T) {
	handler := newTestServer()

	put := httptest.NewRequest(http.MethodPut, "/kv/language", bytes.NewBufferString(`{"value":"go"}`))
	put.Header.Set("Content-Type", "application/json")
	putResponse := httptest.NewRecorder()
	handler.ServeHTTP(putResponse, put)
	if putResponse.Code != http.StatusOK {
		t.Fatalf("expected PUT status 200, got %d: %s", putResponse.Code, putResponse.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/kv/language", nil)
	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK {
		t.Fatalf("expected GET status 200, got %d: %s", getResponse.Code, getResponse.Body.String())
	}

	var got keyResponse
	if err := json.NewDecoder(getResponse.Body).Decode(&got); err != nil {
		t.Fatalf("decode GET response: %v", err)
	}
	if got.Key != "language" || got.Value != "go" {
		t.Fatalf("unexpected GET response: %#v", got)
	}

	deleteRequest := httptest.NewRequest(http.MethodDelete, "/kv/language", nil)
	deleteResponse := httptest.NewRecorder()
	handler.ServeHTTP(deleteResponse, deleteRequest)
	if deleteResponse.Code != http.StatusNoContent {
		t.Fatalf("expected DELETE status 204, got %d: %s", deleteResponse.Code, deleteResponse.Body.String())
	}

	missing := httptest.NewRequest(http.MethodGet, "/kv/language", nil)
	missingResponse := httptest.NewRecorder()
	handler.ServeHTTP(missingResponse, missing)
	if missingResponse.Code != http.StatusNotFound {
		t.Fatalf("expected missing GET status 404, got %d", missingResponse.Code)
	}
}

func TestLogEndpointReflectsMutations(t *testing.T) {
	handler := newTestServer()

	requests := []*http.Request{
		httptest.NewRequest(http.MethodPut, "/kv/language", bytes.NewBufferString(`{"value":"go"}`)),
		httptest.NewRequest(http.MethodDelete, "/kv/language", nil),
	}
	for _, request := range requests {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code < 200 || response.Code >= 300 {
			t.Fatalf("expected mutation success, got %d: %s", response.Code, response.Body.String())
		}
	}

	logRequest := httptest.NewRequest(http.MethodGet, "/log", nil)
	logResponse := httptest.NewRecorder()
	handler.ServeHTTP(logResponse, logRequest)
	if logResponse.Code != http.StatusOK {
		t.Fatalf("expected log status 200, got %d", logResponse.Code)
	}

	var body struct {
		Entries []oplog.Entry `json:"entries"`
	}
	if err := json.NewDecoder(logResponse.Body).Decode(&body); err != nil {
		t.Fatalf("decode log response: %v", err)
	}
	if len(body.Entries) != 2 {
		t.Fatalf("expected 2 log entries, got %d", len(body.Entries))
	}
	if body.Entries[0].Index != 1 || body.Entries[1].Index != 2 {
		t.Fatalf("unexpected log indexes: %#v", body.Entries)
	}
}

func TestMissingDeleteReturnsNotFound(t *testing.T) {
	handler := newTestServer()

	request := httptest.NewRequest(http.MethodDelete, "/kv/missing", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("expected DELETE missing status 404, got %d", response.Code)
	}
}

func newTestServer() http.Handler {
	kv := store.NewMemory()
	members := []cluster.Member{
		{ID: "node-test", Address: "localhost:8080"},
	}
	state := cluster.NewState("node-test", "node-test", members)
	log := oplog.New()
	raft := raftstate.NewState("node-test")
	raft.BecomeCandidate()
	raft.BecomeLeader()
	faultsState := faults.NewState()
	applier := apply.NewApplier(log, kv)
	return NewServer(kv, state, log, raft, faultsState, applier).Routes()
}

func newFollowerTestServer() http.Handler {
	kv := store.NewMemory()
	members := []cluster.Member{
		{ID: "node-1", Address: "http://localhost:8080"},
		{ID: "node-2", Address: "http://localhost:8081"},
	}
	state := cluster.NewState("node-2", "node-1", members)
	log := oplog.New()
	raft := raftstate.NewState("node-2")
	faultsState := faults.NewState()
	applier := apply.NewApplier(log, kv)
	return NewServer(kv, state, log, raft, faultsState, applier).Routes()
}

func TestReplicateAppliesEntryOnFollower(t *testing.T) {
	handler := newFollowerTestServer()

	body := bytes.NewBufferString(`{
		"entry": {
			"index": 1,
			"operation": "put",
			"key": "language",
			"value": "go",
			"createdAt": "2026-08-18T00:00:00Z"
		},
		"leaderCommit": 1
	}`)

	request := httptest.NewRequest(http.MethodPost, "/internal/replicate", body)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected replication status 200, got %d: %s", response.Code, response.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/kv/language", nil)
	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, get)

	if getResponse.Code != http.StatusOK {
		t.Fatalf("expected replicated key to be readable, got %d", getResponse.Code)
	}
}

func TestLeaderRejectsReplicatedEntry(t *testing.T) {
	handler := newTestServer()

	body := bytes.NewBufferString(`{
		"index": 1,
		"operation": "put",
		"key": "language",
		"value": "go",
		"createdAt": "2026-08-18T00:00:00Z"
	}`)

	request := httptest.NewRequest(http.MethodPost, "/internal/replicate", body)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected leader replication status 400, got %d", response.Code)
	}
}

func TestInternalLogEntriesReturnsEntriesAfterIndex(t *testing.T) {
	handler := newTestServer()

	putA := httptest.NewRequest(http.MethodPut, "/kv/a", bytes.NewBufferString(`{"value":"1"}`))
	putB := httptest.NewRequest(http.MethodPut, "/kv/b", bytes.NewBufferString(`{"value":"2"}`))

	for _, request := range []*http.Request{putA, putB} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("expected PUT status 200, got %d", response.Code)
		}
	}

	request := httptest.NewRequest(http.MethodGet, "/internal/log?after=1", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected internal log status 200, got %d", response.Code)
	}

	var body struct {
		Entries []oplog.Entry `json:"entries"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(body.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(body.Entries))
	}

	if body.Entries[0].Index != 2 || body.Entries[0].Key != "b" {
		t.Fatalf("unexpected entry: %#v", body.Entries[0])
	}
}

func TestFollowerRejectsInternalLogRequest(t *testing.T) {
	handler := newFollowerTestServer()

	request := httptest.NewRequest(http.MethodGet, "/internal/log?after=0", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("expected follower internal log status 403, got %d", response.Code)
	}
}

func TestRequestVoteEndpoint(t *testing.T) {
	handler := newTestServer()

	body := bytes.NewBufferString(`{
		"term": 2,
		"candidateId": "node-2"
	}`)

	request := httptest.NewRequest(http.MethodPost, "/internal/raft/request-vote", body)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected request vote status 200, got %d", response.Code)
	}

	var vote raftstate.RequestVoteResponse
	if err := json.NewDecoder(response.Body).Decode(&vote); err != nil {
		t.Fatalf("decode vote response: %v", err)
	}

	if !vote.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if vote.Term != 2 {
		t.Fatalf("expected term 2, got %d", vote.Term)
	}
}

func TestAppendEntriesEndpoint(t *testing.T) {
	handler := newFollowerTestServer()

	body := bytes.NewBufferString(`{
		"term": 1,
		"leaderId": "node-1"
	}`)

	request := httptest.NewRequest(http.MethodPost, "/internal/raft/append-entries", body)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected append entries status 200, got %d", response.Code)
	}

	var heartbeat raftstate.AppendEntriesResponse
	if err := json.NewDecoder(response.Body).Decode(&heartbeat); err != nil {
		t.Fatalf("decode append entries response: %v", err)
	}

	if !heartbeat.Success {
		t.Fatal("expected heartbeat to succeed")
	}

	if heartbeat.Term != 1 {
		t.Fatalf("expected term 1, got %d", heartbeat.Term)
	}
}

func TestRaftStateEndpoint(t *testing.T) {
	handler := newTestServer()

	request := httptest.NewRequest(http.MethodGet, "/raft", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected raft status 200, got %d", response.Code)
	}

	var body raftstate.Snapshot
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode raft state: %v", err)
	}

	if body.NodeID != "node-test" {
		t.Fatalf("expected node-test, got %q", body.NodeID)
	}
}

func TestClusterEndpointUsesRaftLeader(t *testing.T) {
	kv := store.NewMemory()
	members := []cluster.Member{
		{ID: "node-1", Address: "http://localhost:8080"},
		{ID: "node-2", Address: "http://localhost:8081"},
	}
	clusterState := cluster.NewState("node-1", "node-1", members)
	log := oplog.New()

	raft := raftstate.NewState("node-1")
	raft.BecomeFollower(2, "node-2")

	faultsState := faults.NewState()
	applier := apply.NewApplier(log, kv)
	handler := NewServer(kv, clusterState, log, raft, faultsState, applier).Routes()

	request := httptest.NewRequest(http.MethodGet, "/cluster", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected cluster status 200, got %d", response.Code)
	}

	var body struct {
		Members []cluster.Member `json:"members"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode cluster response: %v", err)
	}

	if body.Members[0].Role != cluster.RoleFollower {
		t.Fatalf("expected node-1 follower, got %q", body.Members[0].Role)
	}

	if body.Members[1].Role != cluster.RoleLeader {
		t.Fatalf("expected node-2 leader, got %q", body.Members[1].Role)
	}
}

func TestLeaderSkipsReplicationToFaultedPeer(t *testing.T) {
	kv := store.NewMemory()
	log := oplog.New()

	faultState := faults.NewState()
	faultState.DropReplicationTo("node-3")

	healthyFollower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer healthyFollower.Close()

	faultedFollowerCalled := false
	faultedFollower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		faultedFollowerCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer faultedFollower.Close()

	members := []cluster.Member{
		{ID: "node-1", Address: "http://localhost:8080"},
		{ID: "node-2", Address: healthyFollower.URL},
		{ID: "node-3", Address: faultedFollower.URL},
	}
	clusterState := cluster.NewState("node-1", "node-1", members)

	raft := raftstate.NewState("node-1")
	raft.BecomeCandidate()
	raft.BecomeLeader()

	applier := apply.NewApplier(log, kv)
	handler := NewServer(kv, clusterState, log, raft, faultState, applier).Routes()

	request := httptest.NewRequest(http.MethodPut, "/kv/faulted", bytes.NewBufferString(`{"value":"dropped"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected PUT status 200, got %d: %s", response.Code, response.Body.String())
	}

	if faultedFollowerCalled {
		t.Fatal("expected faulted follower not to receive replication")
	}
}

func TestFaultEndpointsDropAndHealReplication(t *testing.T) {
	handler := newTestServer()

	drop := httptest.NewRequest(http.MethodPost, "/faults/replication/node-3", nil)
	dropResponse := httptest.NewRecorder()
	handler.ServeHTTP(dropResponse, drop)

	if dropResponse.Code != http.StatusOK {
		t.Fatalf("expected drop status 200, got %d", dropResponse.Code)
	}

	list := httptest.NewRequest(http.MethodGet, "/faults", nil)
	listResponse := httptest.NewRecorder()
	handler.ServeHTTP(listResponse, list)

	if listResponse.Code != http.StatusOK {
		t.Fatalf("expected faults status 200, got %d", listResponse.Code)
	}

	if !strings.Contains(listResponse.Body.String(), "node-3") {
		t.Fatalf("expected node-3 fault in response, got %s", listResponse.Body.String())
	}

	heal := httptest.NewRequest(http.MethodDelete, "/faults/replication/node-3", nil)
	healResponse := httptest.NewRecorder()
	handler.ServeHTTP(healResponse, heal)

	if healResponse.Code != http.StatusOK {
		t.Fatalf("expected heal status 200, got %d", healResponse.Code)
	}
}

func TestFailedMajorityRollsBackPut(t *testing.T) {
	kv := store.NewMemory()
	log := oplog.New()
	faultState := faults.NewState()

	members := []cluster.Member{
		{ID: "node-1", Address: "http://localhost:8080"},
		{ID: "node-2", Address: "http://127.0.0.1:1"},
		{ID: "node-3", Address: "http://127.0.0.1:1"},
	}
	clusterState := cluster.NewState("node-1", "node-1", members)

	raft := raftstate.NewState("node-1")
	raft.BecomeCandidate()
	raft.BecomeLeader()

	applier := apply.NewApplier(log, kv)
	handler := NewServer(kv, clusterState, log, raft, faultState, applier).Routes()

	request := httptest.NewRequest(http.MethodPut, "/kv/failed", bytes.NewBufferString(`{"value":"no-majority"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected PUT status 503, got %d: %s", response.Code, response.Body.String())
	}

	if log.LastIndex() != 0 {
		t.Fatalf("expected log rollback to index 0, got %d", log.LastIndex())
	}

	get := httptest.NewRequest(http.MethodGet, "/kv/failed", nil)
	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, get)

	if getResponse.Code != http.StatusNotFound {
		t.Fatalf("expected rolled back key to be missing, got %d", getResponse.Code)
	}
}

func TestInternalLogRejectsPartitionedNode(t *testing.T) {
	kv := store.NewMemory()
	log := oplog.New()

	faultState := faults.NewState()
	faultState.DropReplicationTo("node-3")

	members := []cluster.Member{
		{ID: "node-1", Address: "http://localhost:8080"},
		{ID: "node-3", Address: "http://localhost:8082"},
	}
	clusterState := cluster.NewState("node-1", "node-1", members)

	raft := raftstate.NewState("node-1")
	raft.BecomeCandidate()
	raft.BecomeLeader()

	applier := apply.NewApplier(log, kv)
	handler := NewServer(kv, clusterState, log, raft, faultState, applier).Routes()

	request := httptest.NewRequest(http.MethodGet, "/internal/log?after=0", nil)
	request.Header.Set("X-Node-ID", "node-3")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected partitioned catch-up status 503, got %d", response.Code)
	}
}

func TestReplicateCommitAppliesExistingEntryOnFollower(t *testing.T) {
	handler := newFollowerTestServer()

	appendOnly := bytes.NewBufferString(`{
		"entry": {
			"index": 1,
			"operation": "put",
			"key": "language",
			"value": "go",
			"createdAt": "2026-08-18T00:00:00Z"
		},
		"leaderCommit": 0
	}`)

	appendRequest := httptest.NewRequest(http.MethodPost, "/internal/replicate", appendOnly)
	appendResponse := httptest.NewRecorder()
	handler.ServeHTTP(appendResponse, appendRequest)

	if appendResponse.Code != http.StatusOK {
		t.Fatalf("expected append replication status 200, got %d: %s", appendResponse.Code, appendResponse.Body.String())
	}

	getBeforeCommit := httptest.NewRequest(http.MethodGet, "/kv/language", nil)
	getBeforeCommitResponse := httptest.NewRecorder()
	handler.ServeHTTP(getBeforeCommitResponse, getBeforeCommit)

	if getBeforeCommitResponse.Code != http.StatusNotFound {
		t.Fatalf("expected uncommitted key to be hidden, got %d", getBeforeCommitResponse.Code)
	}

	commitOnly := bytes.NewBufferString(`{
		"leaderCommit": 1
	}`)

	commitRequest := httptest.NewRequest(http.MethodPost, "/internal/replicate", commitOnly)
	commitResponse := httptest.NewRecorder()
	handler.ServeHTTP(commitResponse, commitRequest)

	if commitResponse.Code != http.StatusOK {
		t.Fatalf("expected commit replication status 200, got %d: %s", commitResponse.Code, commitResponse.Body.String())
	}

	getAfterCommit := httptest.NewRequest(http.MethodGet, "/kv/language", nil)
	getAfterCommitResponse := httptest.NewRecorder()
	handler.ServeHTTP(getAfterCommitResponse, getAfterCommit)

	if getAfterCommitResponse.Code != http.StatusOK {
		t.Fatalf("expected committed key to be readable, got %d", getAfterCommitResponse.Code)
	}
}
