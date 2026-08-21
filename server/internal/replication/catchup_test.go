package replication

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kvstore/internal/oplog"
	"kvstore/internal/store"
)

func TestCatchUpFetchesAndAppliesMissingEntries(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after") != "1" {
			t.Fatalf("expected after=1, got %q", r.URL.Query().Get("after"))
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"entries": []oplog.Entry{
				{
					Index:     2,
					Operation: oplog.OperationPut,
					Key:       "language",
					Value:     "go",
					CreatedAt: time.Now().UTC(),
				},
			},
			"lastIndex": 2,
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

	if err := CatchUp(context.Background(), "node-2", leader.URL, log, kv); err != nil {
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
		if r.URL.Query().Get("after") != "2" {
			t.Fatalf("expected after=2, got %q", r.URL.Query().Get("after"))
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"entries":   []oplog.Entry{},
			"lastIndex": 1,
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

	if err := CatchUp(context.Background(), "node-2", leader.URL, log, kv); err != nil {
		t.Fatalf("catch up: %v", err)
	}

	if log.LastIndex() != 1 {
		t.Fatalf("expected log to truncate to index 1, got %d", log.LastIndex())
	}

	if _, err := kv.Get("extra"); err != store.ErrNotFound {
		t.Fatalf("expected extra key to be removed, got %v", err)
	}
}
