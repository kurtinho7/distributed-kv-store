package replication

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kvstore/internal/apply"
	"kvstore/internal/oplog"
	"kvstore/internal/store"
)

func TestCatchUpFetchesAndAppliesMissingEntries(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after") != "0" {
			t.Fatalf("expected after=0, got %q", r.URL.Query().Get("after"))
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"entries": []oplog.Entry{
				{
					Index:     1,
					Operation: oplog.OperationPut,
					Key:       "phase",
					Value:     "two",
					CreatedAt: time.Now().UTC(),
				},
				{
					Index:     2,
					Operation: oplog.OperationPut,
					Key:       "language",
					Value:     "go",
					CreatedAt: time.Now().UTC(),
				},
			},
			"lastIndex":   2,
			"commitIndex": 2,
		})
	}))
	defer leader.Close()

	log := oplog.New()
	if _, err := log.Append(oplog.OperationPut, "phase", "two"); err != nil {
		t.Fatalf("append existing entry: %v", err)
	}
	kv := store.NewMemory()
	if err := kv.Apply(log.Entries()[0]); err != nil {
		t.Fatalf("apply existing entry: %v", err)
	}

	applier := apply.NewApplier(log, kv)

	if err := CatchUp(context.Background(), "node-2", leader.URL, log, kv, applier); err != nil {
		t.Fatalf("catch up: %v", err)
	}

	if log.LastIndex() != 2 {
		t.Fatalf("expected last index 2, got %d", log.LastIndex())
	}

	value, err := kv.Get("language")
	if err != nil {
		t.Fatalf("expected caught-up key: %v", err)
	}
	if value != "go" {
		t.Fatalf("expected go, got %q", value)
	}
}

func TestCatchUpTruncatesEntriesBeyondLeaderLastIndex(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after") != "0" {
			t.Fatalf("expected after=0, got %q", r.URL.Query().Get("after"))
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"entries": []oplog.Entry{
				{Index: 1, Operation: oplog.OperationPut, Key: "committed", Value: "yes"},
			},
			"lastIndex":   1,
			"commitIndex": 1,
		})
	}))
	defer leader.Close()

	log := oplog.New()
	if _, err := log.Append(oplog.OperationPut, "committed", "yes"); err != nil {
		t.Fatalf("append committed entry: %v", err)
	}
	if _, err := log.Append(oplog.OperationPut, "extra", "no"); err != nil {
		t.Fatalf("append extra entry: %v", err)
	}

	kv := store.NewMemory()
	if err := kv.Rebuild(log.Entries()); err != nil {
		t.Fatalf("rebuild store: %v", err)
	}

	applier := apply.NewApplier(log, kv)
	if err := CatchUp(context.Background(), "node-2", leader.URL, log, kv, applier); err != nil {
		t.Fatalf("catch up: %v", err)
	}

	if log.LastIndex() != 1 {
		t.Fatalf("expected log to truncate to index 1, got %d", log.LastIndex())
	}

	if _, err := kv.Get("extra"); err != store.ErrNotFound {
		t.Fatalf("expected extra key to be removed, got %v", err)
	}
}

func TestCatchUpTruncatesConflictingEntryAtSameIndex(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"entries": []oplog.Entry{
				{Index: 1, Operation: oplog.OperationPut, Key: "stable", Value: "same"},
				{Index: 2, Operation: oplog.OperationPut, Key: "leader", Value: "wins"},
			},
			"lastIndex":   2,
			"commitIndex": 2,
		})
	}))
	defer leader.Close()

	log := oplog.New()
	if _, err := log.Append(oplog.OperationPut, "stable", "same"); err != nil {
		t.Fatalf("append stable entry: %v", err)
	}
	if _, err := log.Append(oplog.OperationPut, "follower", "loses"); err != nil {
		t.Fatalf("append conflicting entry: %v", err)
	}

	kv := store.NewMemory()
	applier := apply.NewApplier(log, kv)
	if err := applier.AdvanceCommit(2); err != nil {
		t.Fatalf("advance local commit: %v", err)
	}

	if err := CatchUp(context.Background(), "node-2", leader.URL, log, kv, applier); err != nil {
		t.Fatalf("catch up: %v", err)
	}

	if _, err := kv.Get("follower"); err != store.ErrNotFound {
		t.Fatalf("expected conflicting follower key removed, got %v", err)
	}

	value, err := kv.Get("leader")
	if err != nil {
		t.Fatalf("expected leader key to be applied: %v", err)
	}
	if value != "wins" {
		t.Fatalf("expected leader=wins, got %q", value)
	}
}
