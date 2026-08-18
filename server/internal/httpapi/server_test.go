package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"kvstore/internal/cluster"
	"kvstore/internal/oplog"
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
	return NewServer(kv, state, log).Routes()
}

func newFollowerTestServer() http.Handler {
	kv := store.NewMemory()
	members := []cluster.Member{
		{ID: "node-1", Address: "http://localhost:8080"},
		{ID: "node-2", Address: "http://localhost:8081"},
	}
	state := cluster.NewState("node-2", "node-1", members)
	log := oplog.New()
	return NewServer(kv, state, log).Routes()
}

func TestReplicateAppliesEntryOnFollower(t *testing.T) {
	handler := newFollowerTestServer()

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
