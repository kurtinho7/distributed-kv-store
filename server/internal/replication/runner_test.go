package replication

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kvstore/internal/cluster"
	"kvstore/internal/oplog"
	"kvstore/internal/raft"
	"kvstore/internal/store"
)

func TestCatchUpRunnerFetchesFromKnownLeader(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"entries": []oplog.Entry{
				{
					Index:     1,
					Operation: oplog.OperationPut,
					Key:       "healed",
					Value:     "true",
					CreatedAt: time.Now().UTC(),
				},
			},
		})
	}))
	defer leader.Close()

	clusterState := cluster.NewState("node-2", "node-1", []cluster.Member{
		{ID: "node-1", Address: leader.URL},
		{ID: "node-2", Address: "http://localhost:8081"},
	})

	raftState := raft.NewState("node-2")
	raftState.AppendEntries(raft.AppendEntriesRequest{
		Term:     1,
		LeaderID: "node-1",
	})

	log := oplog.New()
	kv := store.NewMemory()

	runner := NewCatchUpRunner(clusterState, raftState, log, kv)
	runner.catchUp(context.Background())

	value, err := kv.Get("healed")
	if err != nil {
		t.Fatalf("expected caught-up key: %v", err)
	}

	if value != "true" {
		t.Fatalf("expected true, got %q", value)
	}
}