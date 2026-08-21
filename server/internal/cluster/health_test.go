package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthCheckerMarksPeerHealthy(t *testing.T) {
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer peerServer.Close()

	state := NewState("node-1", "node-1", []Member{
		{ID: "node-1", Address: "http://localhost:8080"},
		{ID: "node-2", Address: peerServer.URL},
	})

	checker := NewHealthChecker(state)
	checker.check(context.Background())

	if !state.IsHealthy("node-2") {
		t.Fatal("expected peer to be healthy")
	}
}

func TestHealthCheckerMarksPeerUnhealthy(t *testing.T) {
	state := NewState("node-1", "node-1", []Member{
		{ID: "node-1", Address: "http://localhost:8080"},
		{ID: "node-2", Address: "http://127.0.0.1:1"},
	})

	checker := NewHealthChecker(state)
	checker.check(context.Background())

	if state.IsHealthy("node-2") {
		t.Fatal("expected peer to be unhealthy")
	}
}

func TestHealthCheckerKeepsLocalNodeHealthy(t *testing.T) {
	state := NewState("node-1", "node-1", []Member{
		{ID: "node-1", Address: "http://127.0.0.1:1"},
	})

	state.SetHealth("node-1", false)

	checker := NewHealthChecker(state)
	checker.check(context.Background())

	if !state.IsHealthy("node-1") {
		t.Fatal("expected local node to be healthy")
	}
}